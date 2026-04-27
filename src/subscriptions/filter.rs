//! Pub/Sub subscription filter language.
//!
//! Implements the subset of the [Pub/Sub filter syntax][syntax] our pipeline relies on:
//! attribute equality (`attributes.k = "v"`, `attributes.k != "v"`), key-presence
//! (`attributes:k`), `hasPrefix(attributes.k, "v")`, and the boolean operators
//! `AND`, `OR`, `NOT` with parentheses. NOT binds tighter than AND, AND tighter than OR.
//!
//! Strings are double-quoted with `\\` and `\"` escapes. Identifiers (attribute keys) match
//! `[A-Za-z_][A-Za-z0-9_-]*`.
//!
//! [syntax]: https://cloud.google.com/pubsub/docs/subscription-message-filter

use std::collections::HashMap;
use std::fmt;

/// A parsed filter expression that can be evaluated against a message's attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    expr: Expr,
}

/// Reasons a filter expression failed to parse. Each variant carries enough context to
/// surface a useful `INVALID_ARGUMENT` to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterParseError {
    UnexpectedToken { found: String, position: usize },
    UnexpectedEnd,
    UnterminatedString,
    InvalidEscape(char),
    InvalidIdentifier(String),
}

impl fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterParseError::UnexpectedToken { found, position } => {
                write!(f, "unexpected token '{}' at position {}", found, position)
            }
            FilterParseError::UnexpectedEnd => f.write_str("unexpected end of filter"),
            FilterParseError::UnterminatedString => f.write_str("unterminated string literal"),
            FilterParseError::InvalidEscape(c) => write!(f, "invalid escape sequence '\\{}'", c),
            FilterParseError::InvalidIdentifier(s) => write!(f, "invalid identifier '{}'", s),
        }
    }
}

impl std::error::Error for FilterParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Expr {
    Eq {
        key: String,
        value: String,
        negated: bool,
    },
    HasPrefix {
        key: String,
        value: String,
    },
    HasAttr {
        key: String,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

impl Filter {
    /// Parses a filter expression. Empty / whitespace-only input is rejected because callers
    /// should pass `None` instead of "" for the absence of a filter.
    pub fn parse(input: &str) -> Result<Self, FilterParseError> {
        let tokens = tokenize(input)?;
        let mut parser = Parser { tokens, pos: 0 };
        let expr = parser.parse_or()?;
        parser.expect_end()?;
        Ok(Filter { expr })
    }

    /// Evaluates the filter against a message's attributes. Returns `true` when the message
    /// should be delivered.
    pub fn matches(&self, attributes: Option<&HashMap<String, String>>) -> bool {
        eval(&self.expr, attributes)
    }
}

fn eval(expr: &Expr, attrs: Option<&HashMap<String, String>>) -> bool {
    match expr {
        Expr::Eq {
            key,
            value,
            negated,
        } => {
            let attr_value = attrs.and_then(|a| a.get(key));
            let eq = attr_value.map(|v| v == value).unwrap_or(false);
            if *negated { !eq } else { eq }
        }
        Expr::HasPrefix { key, value } => attrs
            .and_then(|a| a.get(key))
            .map(|v| v.starts_with(value))
            .unwrap_or(false),
        Expr::HasAttr { key } => attrs.map(|a| a.contains_key(key)).unwrap_or(false),
        Expr::And(l, r) => eval(l, attrs) && eval(r, attrs),
        Expr::Or(l, r) => eval(l, attrs) || eval(r, attrs),
        Expr::Not(inner) => !eval(inner, attrs),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    LParen,
    RParen,
    Comma,
    Eq,
    NotEq,
    And,
    Or,
    Not,
    Attribute(String),    // `attributes.key`
    AttributeHas(String), // `attributes:key`
    Ident(String),
    Str(String),
}

fn tokenize(input: &str) -> Result<Vec<(Token, usize)>, FilterParseError> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' => {
                i += 1;
            }
            b'(' => {
                out.push((Token::LParen, i));
                i += 1;
            }
            b')' => {
                out.push((Token::RParen, i));
                i += 1;
            }
            b',' => {
                out.push((Token::Comma, i));
                i += 1;
            }
            b'=' => {
                out.push((Token::Eq, i));
                i += 1;
            }
            b'!' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                out.push((Token::NotEq, i));
                i += 2;
            }
            b'"' => {
                let start = i;
                i += 1;
                let mut s = String::new();
                loop {
                    if i >= bytes.len() {
                        return Err(FilterParseError::UnterminatedString);
                    }
                    match bytes[i] {
                        b'"' => {
                            i += 1;
                            break;
                        }
                        b'\\' => {
                            if i + 1 >= bytes.len() {
                                return Err(FilterParseError::UnterminatedString);
                            }
                            let esc = bytes[i + 1] as char;
                            match esc {
                                '"' => s.push('"'),
                                '\\' => s.push('\\'),
                                _ => return Err(FilterParseError::InvalidEscape(esc)),
                            }
                            i += 2;
                        }
                        b => {
                            s.push(b as char);
                            i += 1;
                        }
                    }
                }
                out.push((Token::Str(s), start));
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'-')
                {
                    i += 1;
                }
                let word = std::str::from_utf8(&bytes[start..i])
                    .map_err(|_| FilterParseError::InvalidIdentifier("<non-utf8>".to_string()))?
                    .to_string();
                if word.eq_ignore_ascii_case("and") {
                    out.push((Token::And, start));
                } else if word.eq_ignore_ascii_case("or") {
                    out.push((Token::Or, start));
                } else if word.eq_ignore_ascii_case("not") {
                    out.push((Token::Not, start));
                } else if word == "attributes" {
                    if i >= bytes.len() {
                        return Err(FilterParseError::UnexpectedEnd);
                    }
                    let sep = bytes[i];
                    if sep != b'.' && sep != b':' {
                        return Err(FilterParseError::UnexpectedToken {
                            found: format!("attributes{}", sep as char),
                            position: i,
                        });
                    }
                    i += 1;
                    let key_start = i;
                    while i < bytes.len()
                        && (bytes[i].is_ascii_alphanumeric()
                            || bytes[i] == b'_'
                            || bytes[i] == b'-')
                    {
                        i += 1;
                    }
                    if key_start == i {
                        return Err(FilterParseError::InvalidIdentifier(
                            "empty attribute key".to_string(),
                        ));
                    }
                    let key = std::str::from_utf8(&bytes[key_start..i])
                        .map_err(|_| FilterParseError::InvalidIdentifier("<non-utf8>".to_string()))?
                        .to_string();
                    out.push((
                        if sep == b'.' {
                            Token::Attribute(key)
                        } else {
                            Token::AttributeHas(key)
                        },
                        start,
                    ));
                } else {
                    out.push((Token::Ident(word), start));
                }
            }
            other => {
                return Err(FilterParseError::UnexpectedToken {
                    found: (other as char).to_string(),
                    position: i,
                });
            }
        }
    }

    Ok(out)
}

struct Parser {
    tokens: Vec<(Token, usize)>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(t, _)| t)
    }

    fn advance(&mut self) -> Option<(Token, usize)> {
        let item = self.tokens.get(self.pos).cloned();
        if item.is_some() {
            self.pos += 1;
        }
        item
    }

    fn expect_end(&self) -> Result<(), FilterParseError> {
        if self.pos < self.tokens.len() {
            let (tok, pos) = &self.tokens[self.pos];
            return Err(FilterParseError::UnexpectedToken {
                found: format!("{:?}", tok),
                position: *pos,
            });
        }
        Ok(())
    }

    fn parse_or(&mut self) -> Result<Expr, FilterParseError> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Token::Or)) {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, FilterParseError> {
        let mut left = self.parse_not()?;
        while matches!(self.peek(), Some(Token::And)) {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, FilterParseError> {
        if matches!(self.peek(), Some(Token::Not)) {
            self.advance();
            let inner = self.parse_not()?;
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, FilterParseError> {
        let (tok, pos) = self.advance().ok_or(FilterParseError::UnexpectedEnd)?;
        match tok {
            Token::LParen => {
                let inner = self.parse_or()?;
                match self.advance() {
                    Some((Token::RParen, _)) => Ok(inner),
                    Some((other, p)) => Err(FilterParseError::UnexpectedToken {
                        found: format!("{:?}", other),
                        position: p,
                    }),
                    None => Err(FilterParseError::UnexpectedEnd),
                }
            }
            Token::Attribute(key) => {
                // Either `attributes.k = "v"` or `attributes.k != "v"`.
                let (op, _op_pos) = self.advance().ok_or(FilterParseError::UnexpectedEnd)?;
                let negated = match op {
                    Token::Eq => false,
                    Token::NotEq => true,
                    other => {
                        return Err(FilterParseError::UnexpectedToken {
                            found: format!("{:?}", other),
                            position: pos,
                        });
                    }
                };
                let value = self.expect_string()?;
                Ok(Expr::Eq {
                    key,
                    value,
                    negated,
                })
            }
            Token::AttributeHas(key) => Ok(Expr::HasAttr { key }),
            Token::Ident(name) if name == "hasPrefix" => {
                self.expect(Token::LParen)?;
                let key = self.expect_attribute()?;
                self.expect(Token::Comma)?;
                let value = self.expect_string()?;
                self.expect(Token::RParen)?;
                Ok(Expr::HasPrefix { key, value })
            }
            other => Err(FilterParseError::UnexpectedToken {
                found: format!("{:?}", other),
                position: pos,
            }),
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), FilterParseError> {
        match self.advance() {
            Some((t, _)) if t == expected => Ok(()),
            Some((t, p)) => Err(FilterParseError::UnexpectedToken {
                found: format!("{:?}", t),
                position: p,
            }),
            None => Err(FilterParseError::UnexpectedEnd),
        }
    }

    fn expect_string(&mut self) -> Result<String, FilterParseError> {
        match self.advance() {
            Some((Token::Str(s), _)) => Ok(s),
            Some((other, p)) => Err(FilterParseError::UnexpectedToken {
                found: format!("{:?}", other),
                position: p,
            }),
            None => Err(FilterParseError::UnexpectedEnd),
        }
    }

    fn expect_attribute(&mut self) -> Result<String, FilterParseError> {
        match self.advance() {
            Some((Token::Attribute(k), _)) => Ok(k),
            Some((other, p)) => Err(FilterParseError::UnexpectedToken {
                found: format!("{:?}", other),
                position: p,
            }),
            None => Err(FilterParseError::UnexpectedEnd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect()
    }

    fn parse(input: &str) -> Filter {
        Filter::parse(input).expect("parse")
    }

    #[test]
    fn parses_simple_equality() {
        let f = parse("attributes.ce-type = \"foo\"");
        assert!(f.matches(Some(&attrs(&[("ce-type", "foo")]))));
        assert!(!f.matches(Some(&attrs(&[("ce-type", "bar")]))));
        assert!(!f.matches(None));
    }

    #[test]
    fn inequality_treats_missing_as_not_equal() {
        let f = parse("attributes.ce-type != \"foo\"");
        assert!(f.matches(Some(&attrs(&[("ce-type", "bar")]))));
        assert!(!f.matches(Some(&attrs(&[("ce-type", "foo")]))));
        // Missing attribute satisfies `!=` per Pub/Sub semantics.
        assert!(f.matches(Some(&attrs(&[]))));
    }

    #[test]
    fn or_combines_two_equalities() {
        let f = parse("attributes.x = \"a\" OR attributes.x = \"b\"");
        assert!(f.matches(Some(&attrs(&[("x", "a")]))));
        assert!(f.matches(Some(&attrs(&[("x", "b")]))));
        assert!(!f.matches(Some(&attrs(&[("x", "c")]))));
    }

    #[test]
    fn and_requires_both() {
        let f = parse("attributes.a = \"1\" AND attributes.b = \"2\"");
        assert!(f.matches(Some(&attrs(&[("a", "1"), ("b", "2")]))));
        assert!(!f.matches(Some(&attrs(&[("a", "1")]))));
    }

    #[test]
    fn not_negates() {
        let f = parse("NOT attributes.x = \"a\"");
        assert!(!f.matches(Some(&attrs(&[("x", "a")]))));
        assert!(f.matches(Some(&attrs(&[("x", "b")]))));
    }

    #[test]
    fn precedence_not_then_and_then_or() {
        // NOT a = X AND b = Y  ==  (NOT (a=X)) AND (b=Y)
        let f = parse("NOT attributes.a = \"X\" AND attributes.b = \"Y\"");
        assert!(f.matches(Some(&attrs(&[("a", "Q"), ("b", "Y")]))));
        assert!(!f.matches(Some(&attrs(&[("a", "X"), ("b", "Y")]))));

        // a=X OR b=Y AND c=Z  ==  a=X OR (b=Y AND c=Z)
        let f = parse("attributes.a = \"X\" OR attributes.b = \"Y\" AND attributes.c = \"Z\"");
        assert!(f.matches(Some(&attrs(&[("a", "X")]))));
        assert!(f.matches(Some(&attrs(&[("b", "Y"), ("c", "Z")]))));
        assert!(!f.matches(Some(&attrs(&[("b", "Y"), ("c", "W")]))));
    }

    #[test]
    fn parens_override_precedence() {
        let f = parse("(attributes.a = \"X\" OR attributes.b = \"Y\") AND attributes.c = \"Z\"");
        assert!(f.matches(Some(&attrs(&[("a", "X"), ("c", "Z")]))));
        assert!(!f.matches(Some(&attrs(&[("a", "X")]))));
    }

    #[test]
    fn has_prefix_function() {
        let f = parse("hasPrefix(attributes.x, \"foo\")");
        assert!(f.matches(Some(&attrs(&[("x", "foobar")]))));
        assert!(!f.matches(Some(&attrs(&[("x", "barfoo")]))));
        assert!(!f.matches(None));
    }

    #[test]
    fn has_attribute() {
        let f = parse("attributes:x");
        assert!(f.matches(Some(&attrs(&[("x", "")]))));
        assert!(f.matches(Some(&attrs(&[("x", "anything")]))));
        assert!(!f.matches(Some(&attrs(&[("y", "z")]))));
    }

    #[test]
    fn handles_escapes_in_string() {
        let f = parse("attributes.x = \"he said \\\"hi\\\"\"");
        assert!(f.matches(Some(&attrs(&[("x", "he said \"hi\"")]))));
    }

    #[test]
    fn rejects_empty_input() {
        assert!(matches!(
            Filter::parse(""),
            Err(FilterParseError::UnexpectedEnd)
        ));
    }

    #[test]
    fn rejects_dangling_operator() {
        assert!(Filter::parse("attributes.x =").is_err());
    }

    #[test]
    fn rejects_unknown_function() {
        assert!(Filter::parse("hasSuffix(attributes.x, \"y\")").is_err());
    }

    #[test]
    fn ce_type_or_equality_matches_pipeline_filter_shape() {
        // Mirrors what cula-platform sends for cloud-events filtering.
        let f =
            parse("attributes.ce-type = \"com.cula.foo\" OR attributes.ce-type = \"com.cula.bar\"");
        assert!(f.matches(Some(&attrs(&[("ce-type", "com.cula.foo")]))));
        assert!(f.matches(Some(&attrs(&[("ce-type", "com.cula.bar")]))));
        assert!(!f.matches(Some(&attrs(&[("ce-type", "com.cula.baz")]))));
    }
}
