use chrono::{NaiveDate, NaiveDateTime};

use super::{ast, error::ParserError};
use std::{iter::Peekable, str::Chars};

#[derive(Debug, PartialEq)]
pub enum Token {
  Keyword(Keyword),
  String(String),
  Number(String),
  Asterisk,
  GreaterThan,
  GreaterThanOrEqual,
  LessThan,
  LessThanOrEqual,
  Equal,
  Not,
  Plus,
  Minus,
  Slash,
  Percent,
  OpenParen,
  CloseParen,
  Comma,
  Semicolon,
  Date(String),
  Timestamp(String),
  Boolean(bool),
  Null,
}

impl Token {
  pub fn to_operator(&self) -> Result<ast::Operator, ParserError> {
    match self {
      Token::Equal => Ok(ast::Operator::Equal),
      Token::Not => Ok(ast::Operator::NotEqual),
      Token::LessThan => Ok(ast::Operator::LessThan),
      Token::LessThanOrEqual => Ok(ast::Operator::LessThanOrEqual),
      Token::GreaterThan => Ok(ast::Operator::GreaterThan),
      Token::GreaterThanOrEqual => Ok(ast::Operator::GreaterThanOrEqual),
      Token::Plus => Ok(ast::Operator::Add),
      Token::Minus => Ok(ast::Operator::Subtract),
      Token::Slash => Ok(ast::Operator::Divide),
      _ => Err(ParserError::UnexpectedToken),
    }
  }
}

#[derive(Debug, PartialEq)]
pub enum Keyword {
  SELECT,
  FROM,
  INSERT,
  AND,
  WHERE,
  OR,
  LIMIT,
  OFFSET,
  GROUP,
  BY,
  ORDER,
  ASC,
  DESC,
  HAVING,
  UPDATE,
  SET,
  DELETE,
  VAULES,
  CREATE,
  TABLE,
  ALTER,
  DROP,
  PRIMARY,
  KEY,
  FOREIGN,
  COLUMN,
  INTO,
  INT,
  DATE,
  TIMESTAMP,
  BOOLEAN,
  NULL,
  NOT,
  UNIQUE,
  REFERENCES,
  CHECK,
  TEXT,
  ADD,
  MODIFY,
  BEGIN,
  COMMIT,
  ROLLBACK,
  TRANSACTION,
}

impl Keyword {
  pub fn from_string(s: &str) -> Option<Keyword> {
    match s.to_uppercase().as_ref() {
      "SELECT" => Some(Keyword::SELECT),
      "WHERE" => Some(Keyword::WHERE),
      "FROM" => Some(Keyword::FROM),
      "INSERT" => Some(Keyword::INSERT),
      "AND" => Some(Keyword::AND),
      "OR" => Some(Keyword::OR),
      "LIMIT" => Some(Keyword::LIMIT),
      "OFFSET" => Some(Keyword::OFFSET),
      "GROUP" => Some(Keyword::GROUP),
      "BY" => Some(Keyword::BY),
      "ORDER" => Some(Keyword::ORDER),
      "ASC" => Some(Keyword::ASC),
      "DESC" => Some(Keyword::DESC),
      "HAVING" => Some(Keyword::HAVING),
      "UPDATE" => Some(Keyword::UPDATE),
      "SET" => Some(Keyword::SET),
      "DELETE" => Some(Keyword::DELETE),
      "VALUES" => Some(Keyword::VAULES),
      "CREATE" => Some(Keyword::CREATE),
      "TABLE" => Some(Keyword::TABLE),
      "ALTER" => Some(Keyword::ALTER),
      "DROP" => Some(Keyword::DROP),
      "PRIMARY" => Some(Keyword::PRIMARY),
      "KEY" => Some(Keyword::KEY),
      "FOREIGN" => Some(Keyword::FOREIGN),
      "COLUMN" => Some(Keyword::COLUMN),
      "INTO" => Some(Keyword::INTO),
      "INT" => Some(Keyword::INT),
      "DATE" => Some(Keyword::DATE),
      "TIMESTAMP" => Some(Keyword::TIMESTAMP),
      "BOOLEAN" => Some(Keyword::BOOLEAN),
      "NULL" => Some(Keyword::NULL),
      "NOT" => Some(Keyword::NOT),
      "UNIQUE" => Some(Keyword::UNIQUE),
      "REFERENCES" => Some(Keyword::REFERENCES),
      "CHECK" => Some(Keyword::CHECK),
      "TEXT" => Some(Keyword::TEXT),
      "ADD" => Some(Keyword::ADD),
      "MODIFY" => Some(Keyword::MODIFY),
      "BEGIN" => Some(Keyword::BEGIN),
      "COMMIT" => Some(Keyword::COMMIT),
      "ROLLBACK" => Some(Keyword::ROLLBACK),
      "TRANSACTION" => Some(Keyword::TRANSACTION),
      _ => None,
    }
  }
}

pub struct Tokenizer<'a> {
  iterator: Peekable<Chars<'a>>,
}

impl<'a> Iterator for Tokenizer<'a> {
  type Item = Token;

  fn next(&mut self) -> Option<Token> {
    self.read()
  }
}

impl<'a> Tokenizer<'a> {
  pub fn new(s: &'a str) -> Self {
    Tokenizer { iterator: s.chars().peekable() }
  }

  fn skip_whitespace(&mut self) {
    self.next_while(|ch| ch.is_whitespace());
  }

  fn next_if(&mut self, condition: impl Fn(char) -> bool) -> Option<char> {
    self.iterator.peek().filter(|&c| condition(*c))?;
    self.iterator.next()
  }

  fn next_while(&mut self, condition: impl Fn(char) -> bool) -> String {
    let mut value = String::new();

    while let Some(ch) = self.next_if(&condition) {
      value.push(ch);
    }

    value
  }

  pub fn read(&mut self) -> Option<Token> {
    self.skip_whitespace();

    match self.iterator.peek().cloned() {
      Some(ch) if ch.is_numeric() => Some(self.read_number()),
      Some(ch) if ch.is_alphabetic() => Some(self.read_keyword_or_string()),
      Some(ch) if ch == '\'' => self.read_string(ch).ok(),
      Some(ch) if is_symbol(ch) => self.read_symbol().ok(),
      Some(_) => None,
      None => None,
    }
  }

  fn read_keyword_or_string(&mut self) -> Token {
    let string = self.next_while(|c| is_keyword_or_identifier(c));

    match string.to_uppercase().as_str() {
      "TRUE" => Token::Boolean(true),
      "FALSE" => Token::Boolean(false),
      "NULL" => Token::Null,
      _ => Keyword::from_string(&string).map_or_else(|| Token::String(string), Token::Keyword),
    }
  }

  fn read_number(&mut self) -> Token {
    let value = self.next_while(|c| c.is_numeric());
    Token::Number(value)
  }

  fn parse_date_or_timestamp(&self, literal: &str) -> Token {
    if NaiveDate::parse_from_str(literal, "%Y-%m-%d").is_ok() {
      return Token::Date(literal.to_string());
    }

    if NaiveDateTime::parse_from_str(literal, "%Y-%m-%d %H:%M:%S").is_ok() {
      return Token::Timestamp(literal.to_string());
    }

    Token::String(literal.to_string())
  }

  fn read_string(&mut self, _first: char) -> Result<Token, ParserError> {
    let mut string = String::new();

    while let Some(&next) = self.iterator.peek() {
      match next {
        'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '\'' | '-' | ':' => {
          string.push(self.iterator.next().unwrap());
        }
        _ => break,
      }
    }

    if string.starts_with('\'') && string.ends_with('\'') {
      let literal = string[1..string.len() - 1].to_string();

      return Ok(self.parse_date_or_timestamp(&literal));
    }


    // dangerous because we can wrongly parse a string as a keyword where it should be column value or name
    match string.to_uppercase().as_str() {
      "SELECT" => Ok(Token::Keyword(Keyword::SELECT)),
      "FROM" => Ok(Token::Keyword(Keyword::FROM)),
      "WHERE" => Ok(Token::Keyword(Keyword::WHERE)),
      "AND" => Ok(Token::Keyword(Keyword::AND)),
      "OR" => Ok(Token::Keyword(Keyword::OR)),
      "OFFSET" => Ok(Token::Keyword(Keyword::OFFSET)),
      "LIMIT" => Ok(Token::Keyword(Keyword::LIMIT)),
      "INSERT" => Ok(Token::Keyword(Keyword::INSERT)),
      "INTO" => Ok(Token::Keyword(Keyword::INTO)),
      "VALUES" => Ok(Token::Keyword(Keyword::VAULES)),
      "UPDATE" => Ok(Token::Keyword(Keyword::UPDATE)),
      "SET" => Ok(Token::Keyword(Keyword::SET)),
      "DELETE" => Ok(Token::Keyword(Keyword::DELETE)),
      "CREATE" => Ok(Token::Keyword(Keyword::CREATE)),
      "TABLE" => Ok(Token::Keyword(Keyword::TABLE)),
      "ALTER" => Ok(Token::Keyword(Keyword::ALTER)),
      "DROP" => Ok(Token::Keyword(Keyword::DROP)),
      "PRIMARY" => Ok(Token::Keyword(Keyword::PRIMARY)),
      "KEY" => Ok(Token::Keyword(Keyword::KEY)),
      "FOREIGN" => Ok(Token::Keyword(Keyword::FOREIGN)),
      "COLUMN" => Ok(Token::Keyword(Keyword::COLUMN)),
      "INT" => Ok(Token::Keyword(Keyword::INT)),
      "DATE" => Ok(Token::Keyword(Keyword::DATE)),
      "TIMESTAMP" => Ok(Token::Keyword(Keyword::TIMESTAMP)),
      "BOOLEAN" => Ok(Token::Keyword(Keyword::BOOLEAN)),
      "NULL" => Ok(Token::Keyword(Keyword::NULL)),
      "NOT" => Ok(Token::Keyword(Keyword::NOT)),
      _ => Ok(Token::String(string)),
    }
  }

  fn read_symbol(&mut self) -> Result<Token, ParserError> {
    match self.iterator.next() {
      Some('*') => Ok(Token::Asterisk),
      Some('>') => self.read_compound_token(Token::GreaterThan, Token::GreaterThanOrEqual),
      Some('<') => self.read_compound_token(Token::LessThan, Token::LessThanOrEqual),
      Some('=') => Ok(Token::Equal),
      Some('!') => Ok(Token::Not),
      Some('+') => Ok(Token::Plus),
      Some('-') => Ok(Token::Minus),
      Some('/') => Ok(Token::Slash),
      Some('%') => Ok(Token::Percent),
      Some('(') => Ok(Token::OpenParen),
      Some(')') => Ok(Token::CloseParen),
      Some(',') => Ok(Token::Comma),
      Some(';') => Ok(Token::Semicolon),
      _ => Err(ParserError::UnexpectedSymbol),
    }
  }

  fn read_compound_token(&mut self, single: Token, compound: Token) -> Result<Token, ParserError> {
    if let Some(&'=') = self.iterator.peek() {
      self.iterator.next();
      Ok(compound)
    } else {
      Ok(single)
    }
  }
}

fn is_keyword_or_identifier(ch: char) -> bool {
  ch.is_alphabetic() || ch == '_'
}

// trafer it to utils.rs
fn is_symbol(ch: char) -> bool {
  match ch {
    '*' | '>' | '<' | '=' | '!' | '+' | '-' | '/' | '%' | '(' | ')' | ',' | ';' => true,
    _ => false,
  }
}
