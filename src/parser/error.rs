use std::fmt;

#[derive(Debug)]
pub enum ParserError {
    UnexpectedToken,
    ExpectedIdentifier,
    ExpectedValue,
    FailedToParseNumber,
    UnexpectedEndOfStream,
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParserError::UnexpectedToken => write!(f, "Unexpected token"),
            ParserError::ExpectedIdentifier => write!(f, "Expected identifier"),
            ParserError::ExpectedValue => write!(f, "Expected value"),
            ParserError::FailedToParseNumber => write!(f, "Failed to parse number"),
            ParserError::UnexpectedEndOfStream => write!(f, "Unexpected end of stream"),
        }
    }
}

impl std::error::Error for ParserError {}