use std::fmt;

#[derive(Debug)]
pub enum ParserError {
  UnexpectedToken,
  ExpectedIdentifier,
  ExpectedValue,
  FailedToParseNumber,
  UnexpectedEndOfStream,
  NoColumnsSpecified,
  FailedToParseDate,
  FailedToParseTimestamp,
  UnexpectedSymbol,
}

impl fmt::Display for ParserError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      ParserError::UnexpectedToken => write!(f, "Unexpected token"),
      ParserError::ExpectedIdentifier => write!(f, "Expected identifier"),
      ParserError::ExpectedValue => write!(f, "Expected value"),
      ParserError::FailedToParseNumber => write!(f, "Failed to parse number"),
      ParserError::UnexpectedEndOfStream => write!(f, "Unexpected end of stream"),
      ParserError::NoColumnsSpecified => write!(f, "No columns specified"),
      ParserError::FailedToParseDate => write!(f, "Failed to parse date"),
      ParserError::FailedToParseTimestamp => write!(f, "Failed to parse timestamp"),
      ParserError::UnexpectedSymbol => write!(f, "Unexpected symbol"),
    }
  }
}

impl std::error::Error for ParserError {}
