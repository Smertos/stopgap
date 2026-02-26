#![forbid(unsafe_code)]

pub mod sql {
    #[must_use]
    pub fn quote_ident(ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    #[must_use]
    pub fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }
}

pub mod settings {
    #[must_use]
    pub fn parse_bool_setting(value: &str) -> Option<bool> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "1" | "on" | "true" | "t" | "yes" | "y" => Some(true),
            "0" | "off" | "false" | "f" | "no" | "n" => Some(false),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(crate::sql::quote_ident("schema\"name"), "\"schema\"\"name\"");
    }

    #[test]
    fn quote_literal_escapes_single_quotes() {
        assert_eq!(crate::sql::quote_literal("it's ok"), "'it''s ok'");
    }

    #[test]
    fn parse_bool_setting_supports_common_postgres_forms() {
        assert_eq!(crate::settings::parse_bool_setting("true"), Some(true));
        assert_eq!(crate::settings::parse_bool_setting("ON"), Some(true));
        assert_eq!(crate::settings::parse_bool_setting("0"), Some(false));
        assert_eq!(crate::settings::parse_bool_setting("no"), Some(false));
        assert_eq!(crate::settings::parse_bool_setting("maybe"), None);
    }
}
