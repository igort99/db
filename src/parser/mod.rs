use ast::{Expression, Literal, Operator};
use error::ParserError;
use tokenizer::{Keyword, Token, Tokenizer};

mod ast;
mod error;
mod tokenizer;

pub struct Parser<'a> {
  tokenizer: std::iter::Peekable<Tokenizer<'a>>,
}

impl<'a> Parser<'a> {
  pub fn new(input: &'a str) -> Parser {
    Parser { tokenizer: Tokenizer::new(input).peekable() }
  }

  pub fn parse(&mut self) -> Result<ast::Statement, Box<dyn std::error::Error>> {
    let statement = self.parse_statement()?;
    Ok(statement)
  }

  fn parse_statement(&mut self) -> Result<ast::Statement, ParserError> {
    match self.tokenizer.peek() {
      Some(Token::Keyword(Keyword::SELECT)) => self.parse_select_statement(),
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_select_columns(&mut self) -> Vec<Expression> {
    let mut select: Vec<Expression> = Vec::new();

    loop {
      let token = self.tokenizer.next().unwrap();

      match token {
        Token::String(name) => select.push(Expression::Identifier(name)),
        Token::Asterisk => select.push(Expression::Identifier("*".to_string())),
        Token::Comma => continue,
        Token::Keyword(Keyword::FROM) => {
          if select.is_empty() {
            panic!("No columns specified for SELECT");
          }
          break;
        }
        _ => panic!("Unexpected token"),
      }
    }

    select
  }

  fn parse_table(&mut self) -> Result<ast::Table, ParserError> {
    let token = match self.tokenizer.next() {
      Some(token) => token,
      None => return Err(ParserError::UnexpectedEndOfStream),
    };

    match token {
      Token::String(name) => Ok(ast::Table { name, alias: None }),
      _ => Err(ParserError::ExpectedIdentifier),
    }
  }

  fn parse_where_clause(&mut self) -> Option<Expression> {
    let token = match self.tokenizer.peek() {
      Some(token) => token,
      None => return None,
    };

    if let Token::Keyword(Keyword::WHERE) = token {
      self.tokenizer.next();

      let mut condition = self.parse_condition();

      while let Some(operator) = self.parse_logical_operator() {
        let right_condition = self.parse_condition();
        condition = Expression::BinaryExpression { left: Box::new(condition), operator, right: Box::new(right_condition) };
      }

      Some(condition)
    } else {
      None
    }
  }

  fn parse_condition(&mut self) -> Expression {
    let left = match self.tokenizer.next().expect("Expected identifier after WHERE") {
      Token::String(name) => Box::new(Expression::Identifier(name)),
      _ => panic!("Expected identifier after WHERE"),
    };

    let operator = self.tokenizer.next().unwrap().to_operator();

    let right = match self.tokenizer.next().expect("Expected value after operator") {
      Token::String(val) => Box::new(Expression::Literal(Literal::String(val))),
      Token::Number(num) => Box::new(Expression::Literal(Literal::Number(num.parse().expect("Failed to parse number")))),
      Token::Date(date) => Box::new(Expression::Literal(Literal::Date(date.parse().expect("Failed to parse timestamp")))),
      Token::Timestamp(date) => {
        Box::new(Expression::Literal(Literal::Timestamp(date.parse().expect("Failed to parse timestamp"))))
      }
      Token::Boolean(val) => Box::new(Expression::Literal(Literal::Boolean(val))),
      Token::Null => Box::new(Expression::Literal(Literal::Null)),
      _ => panic!("Expected value after operator"),
    };

    Expression::BinaryExpression { left, operator, right }
  }

  fn parse_logical_operator(&mut self) -> Option<Operator> {
    match self.tokenizer.peek() {
      Some(Token::Keyword(Keyword::AND)) => {
        self.tokenizer.next();
        Some(Operator::And)
      }
      Some(Token::Keyword(Keyword::OR)) => {
        self.tokenizer.next();
        Some(Operator::Or)
      }
      _ => None,
    }
  }

  fn parse_limit_and_offset(&mut self) -> (Option<Expression>, Option<Expression>) {
    let mut limit = None;
    let mut offset = None;

    while let Some(token) = self.tokenizer.peek() {
      match token {
        Token::Keyword(Keyword::LIMIT) if limit.is_none() => {
          self.tokenizer.next();

          if let Some(Token::Number(num)) = self.tokenizer.next() {
            let number = num.parse::<f64>().expect("Failed to parse number");
            limit = Some(Expression::Literal(Literal::Number(number)));
          } else {
            panic!("Expected number after LIMIT");
          }
        }
        Token::Keyword(Keyword::OFFSET) if offset.is_none() => {
          self.tokenizer.next();

          if let Some(Token::Number(num)) = self.tokenizer.next() {
            let number = num.parse::<f64>().expect("Failed to parse number");
            offset = Some(Expression::Literal(Literal::Number(number)));
          } else {
            panic!("Expected number after OFFSET");
          }
        }
        _ => break,
      }
    }

    (limit, offset)
  }

  fn parse_group_by(&mut self) -> Option<Vec<Expression>> {
    None
  }

  fn parse_having(&mut self) -> Option<Expression> {
    None
  }

  fn parse_order_by(&mut self) -> Option<Vec<(Expression, ast::Order)>> {
    None
  }

  fn parse_select_statement(&mut self) -> Result<ast::Statement, ParserError> {
    self.tokenizer.next();

    let select = self.parse_select_columns();
    let from = self.parse_table()?;

    let where_clause = self.parse_where_clause();
    let group_by = self.parse_group_by();
    let having = self.parse_having();
    let order_by = self.parse_order_by();
    let (limit, offset) = self.parse_limit_and_offset();

    Ok(ast::Statement::Select(ast::SelectStatement { select, from, where_clause, group_by, having, order_by, limit, offset }))
  }
}
