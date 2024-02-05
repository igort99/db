use std::{iter::Peekable, str::Chars};

use super::ast;

#[derive(Debug)]
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
}

impl Token {
  pub fn to_operator(&self) -> ast::Operator {
    match self {
      Token::Equal => ast::Operator::Equal,
      Token::Not => ast::Operator::NotEqual,
      Token::LessThan => ast::Operator::LessThan,
      Token::LessThanOrEqual => ast::Operator::LessThanOrEqual,
      Token::GreaterThan => ast::Operator::GreaterThan,
      Token::GreaterThanOrEqual => ast::Operator::GreaterThanOrEqual,
      _ => panic!("Unexpected token, expected an operator"),
    }
  }
}

#[derive(Debug)]
pub enum Keyword {
  SELECT,
  FROM,
  INSERT,
  AND,
  WHERE,
  OR,
  LIMIT,
  OFFSET,
}

impl Keyword {
  pub fn to_string(&self) -> &str {
    match &self {
      Self::SELECT => "SELECT",
      Self::INSERT => "INSERT",
      Self::FROM => "FROM",
      Self::WHERE => "WHERE",
      Self::AND => "AND",
      Self::OR => "OR",
      Self::LIMIT => "LIMIT",
      Self::OFFSET => "OFFSET",
    }
  }

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
      _ => None,
    }
  }
}

pub struct Tokenizer<'a> {
  iterator: Peekable<Chars<'a>>,
}

impl<'a> Iterator for Tokenizer<'a> {
  type Item = Token;

  fn next(&mut self) -> Option<Self::Item> {
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

    match self.iterator.peek() {
      Some(ch) if ch.is_numeric() => Some(self.read_number()),
      Some(ch) if ch.is_alphabetic() => Some(self.read_keyword_or_string()),
      Some(ch) if is_symbol(*ch) => self.read_symbol(),
      Some(_) => None,
      None => None,
    }
  }

  fn read_keyword_or_string(&mut self) -> Token {
    let value = self.next_while(|c| c.is_alphabetic());

    if let Some(keyword) = Keyword::from_string(&value) {
      Token::Keyword(keyword)
    } else {
      Token::String(value)
    }
  }

  fn read_number(&mut self) -> Token {
    let value = self.next_while(|c| c.is_numeric());
    Token::Number(value)
  }

  fn read_symbol(&mut self) -> Option<Token> {
    match self.iterator.next() {
      Some('*') => Some(Token::Asterisk),
      Some('>') => {
        if let Some(&'=') = self.iterator.peek() {
          self.iterator.next(); // Consume the '='
          Some(Token::GreaterThanOrEqual)
        } else {
          Some(Token::GreaterThan)
        }
      }
      Some('<') => {
        if let Some(&'=') = self.iterator.peek() {
          self.iterator.next(); // Consume the '='
          Some(Token::LessThanOrEqual)
        } else {
          Some(Token::LessThan)
        }
      }
      Some('=') => Some(Token::Equal),
      Some('!') => Some(Token::Not),
      Some('+') => Some(Token::Plus),
      Some('-') => Some(Token::Minus),
      Some('/') => Some(Token::Slash),
      Some('%') => Some(Token::Percent),
      Some('(') => Some(Token::OpenParen),
      Some(')') => Some(Token::CloseParen),
      Some(',') => Some(Token::Comma),
      Some(';') => Some(Token::Semicolon),
      _ => None,
    }
  }
}

// trafer it to utils.rs
fn is_symbol(ch: char) -> bool {
  match ch {
    '*' | '>' | '<' | '=' | '!' | '+' | '-' | '/' | '%' | '(' | ')' | ',' | ';' => true,
    _ => false,
  }
}
