pub fn quote_identifier(identifier: &str) -> String {
    let mut quoted_identifier = String::with_capacity(identifier.len());
    quoted_identifier.push('"');
    for char in identifier.chars() {
        if char == '"' {
            quoted_identifier.push('"');
        }
        quoted_identifier.push(char);
    }
    quoted_identifier.push('"');
    quoted_identifier
}

pub fn quote_literal(literal: &str) -> String {
    let mut quoted_literal = String::with_capacity(literal.len() + 2);

    if literal.find('\\').is_some() {
        quoted_literal.push('E');
    }

    quoted_literal.push('\'');

    for char in literal.chars() {
        if char == '\'' {
            quoted_literal.push('\'');
        } else if char == '\\' {
            quoted_literal.push('\\');
        }

        quoted_literal.push(char);
    }

    quoted_literal.push('\'');

    quoted_literal
}

#[test]
pub fn test_quote_identifier() {
    assert_eq!(quote_identifier(""), r#""""#);
    assert_eq!(quote_identifier("test"), r#""test""#);
    assert_eq!(quote_identifier("TeSt"), r#""TeSt""#);
    assert_eq!(quote_identifier(r#"Te"St"#), r#""Te""St""#);
}
