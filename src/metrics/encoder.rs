use std::fmt::{Display, Write};

/// A tiny writer for the Prometheus text exposition format (version 0.0.4).
///
/// It does not attempt to be a general-purpose registry; it simply renders the
/// metric families we expose, in order.
pub struct PromText {
    buf: String,
}

impl PromText {
    /// Creates a new, empty writer.
    pub fn new() -> Self {
        Self {
            buf: String::with_capacity(1024),
        }
    }

    /// Consumes the writer and returns the rendered text.
    pub fn into_string(self) -> String {
        self.buf
    }

    /// Writes the `# HELP` and `# TYPE` header lines for a metric family.
    pub fn family(&mut self, name: &str, metric_type: &str, help: &str) {
        let _ = writeln!(self.buf, "# HELP {name} {help}");
        let _ = writeln!(self.buf, "# TYPE {name} {metric_type}");
    }

    /// Writes a single sample with no labels.
    pub fn sample(&mut self, name: &str, value: impl Display) {
        let _ = writeln!(self.buf, "{name} {value}");
    }

    /// Writes a single sample with the given labels.
    pub fn labeled(&mut self, name: &str, labels: &[(&str, &str)], value: impl Display) {
        self.buf.push_str(name);
        self.buf.push('{');
        for (i, (key, val)) in labels.iter().enumerate() {
            if i > 0 {
                self.buf.push(',');
            }
            self.buf.push_str(key);
            self.buf.push_str("=\"");
            escape_label_value(&mut self.buf, val);
            self.buf.push('"');
        }
        let _ = writeln!(self.buf, "}} {value}");
    }
}

impl Default for PromText {
    fn default() -> Self {
        Self::new()
    }
}

/// Escapes a label value per the Prometheus text format rules:
/// backslash, double-quote and line feed are escaped.
fn escape_label_value(out: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_family_and_samples() {
        let mut p = PromText::new();
        p.family("deltio_topics", "gauge", "Number of topics.");
        p.sample("deltio_topics", 3);
        p.family("deltio_x", "counter", "X.");
        p.labeled("deltio_x", &[("topic", "projects/p/topics/t")], 7);

        let out = p.into_string();
        assert_eq!(
            out,
            "# HELP deltio_topics Number of topics.\n\
             # TYPE deltio_topics gauge\n\
             deltio_topics 3\n\
             # HELP deltio_x X.\n\
             # TYPE deltio_x counter\n\
             deltio_x{topic=\"projects/p/topics/t\"} 7\n"
        );
    }

    #[test]
    fn escapes_label_values() {
        let mut p = PromText::new();
        p.labeled("m", &[("k", "a\"b\\c\nd")], 1);
        assert_eq!(p.into_string(), "m{k=\"a\\\"b\\\\c\\nd\"} 1\n");
    }
}
