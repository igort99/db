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
      ParserError::UnexpectedToken => write!(f, "Parsing Error: Unexpected token."),
      ParserError::ExpectedIdentifier => write!(f, "Parsing Error: Expected identifier."),
      ParserError::ExpectedValue => write!(f, "Parsing Error: Expected value."),
      ParserError::FailedToParseNumber => write!(f, "Parsing Error: Failed to parse number."),
      ParserError::UnexpectedEndOfStream => write!(f, "Parsing Error: Unexpected end of stream."),
      ParserError::NoColumnsSpecified => write!(f, "Parsing Error: No columns specified."),
      ParserError::FailedToParseDate => write!(f, "Parsing Error: Failed to parse date."),
      ParserError::FailedToParseTimestamp => write!(f, "Parsing Error: Failed to parse timestamp."),
      ParserError::UnexpectedSymbol => write!(f, "Parsing Error: Unexpected symbol."),
    }
  }
}

impl std::error::Error for ParserError {}
