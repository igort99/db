use tokenizer::{Keyword, Token, Tokenizer};

use self::ast::{Expression, Literal};

pub mod ast;
pub mod tokenizer;

pub struct Parser<'a> {
  tokenizer: std::iter::Peekable<Tokenizer<'a>>,
}

impl<'a> Parser<'a> {
  pub fn new(input: &'a str) -> Parser {
    Parser { tokenizer: Tokenizer::new(input).peekable() }
  }

  pub fn parse(&mut self) -> Result<ast::Statement, Box<dyn std::error::Error>> {
    let statement = self.parse_statement();

    Ok(statement)
  }

  fn parse_statement(&mut self) -> ast::Statement {
    match self.tokenizer.peek() {
      Some(Token::Keyword(Keyword::SELECT)) => self.parse_select_statement(),
      _ => panic!("Unexpected token: {:?}", self.tokenizer.peek()),
    }
  }

  fn parse_select_columns(&mut self) -> Vec<String> {
    let mut select: Vec<String> = Vec::new();

    loop {
      let token = self.tokenizer.next().unwrap(); // better to go with expect

      match token {
        Token::String(name) => select.push(name),
        Token::Asterisk => select.push('*'.to_string()),
        Token::Comma => continue,
        Token::Keyword(Keyword::FROM) => break,
        _ => panic!("Unexpected token"),
      }
    }

    select
  }

  fn parse_table(&mut self) -> ast::Table {
    let token = self.tokenizer.next().unwrap(); // better to go with expect

    match token {
      Token::String(name) => ast::Table { name: name, alias: None },
      _ => panic!("Expected table name"), // should change everything for errors so client can know what issue they made
    }
  }

  fn parse_where_clause(&mut self) -> Option<ast::Expression> {
    let token = self.tokenizer.peek().unwrap();

    if let Token::Keyword(Keyword::WHERE) = token {
      self.tokenizer.next(); // If where exist consume it

      let mut conditions = Vec::new();

      loop {
        let left = match self.tokenizer.next().expect("Expected identifier after WHERE") {
          Token::String(name) => Box::new(Expression::Identifier(name)),
          _ => panic!("Expected identifier after WHERE"),
        };

        let operator = self.tokenizer.next().unwrap().to_operator();

        let right = match self.tokenizer.next().expect("Expected value after operator") {
          Token::String(val) => Box::new(Expression::Literal(Literal::String(val))),
          Token::Number(num) => Box::new(Expression::Literal(Literal::Number(num.parse().expect("Failed to parse number")))),
          _ => panic!("Expected value after operator"),
        };

        conditions.push(Expression::BinaryExpression { left, operator, right });

        match self.tokenizer.peek() {
          Some(Token::Keyword(Keyword::AND)) | Some(Token::Keyword(Keyword::OR)) => {
            self.tokenizer.next(); // consume the AND/OR keyword
          }
          _ => break,
        }
      }

      Some(Expression::AndConditions(conditions))
    } else {
      None
    }
  }

  fn parse_select_statement(&mut self) -> ast::Statement {
    self.tokenizer.next();

    let select = self.parse_select_columns();
    let from = self.parse_table();
    let where_clause = self.parse_where_clause();

    ast::Statement::Select(ast::SelectStatement { from, select, where_clause })
  }
}
