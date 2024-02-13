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
      Some(Token::Keyword(Keyword::INSERT)) => self.parse_dml_statement(),
      Some(Token::Keyword(Keyword::UPDATE)) => self.parse_dml_statement(),
      Some(Token::Keyword(Keyword::DELETE)) => self.parse_select_statement(),
      Some(Token::Keyword(Keyword::CREATE)) => self.parse_ddl_statement(),
      Some(Token::Keyword(Keyword::DROP)) => self.parse_ddl_statement(),
      Some(Token::Keyword(Keyword::ALTER)) => self.parse_ddl_statement(),
      Some(Token::Keyword(Keyword::BEGIN)) => self.parse_transaction(),
      Some(Token::Keyword(Keyword::COMMIT)) => self.parse_transaction(),
      Some(Token::Keyword(Keyword::ROLLBACK)) => self.parse_transaction(),
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_select_columns(&mut self) -> Result<Vec<Expression>, ParserError> {
    let mut select: Vec<Expression> = Vec::new();

    while let Some(token) = self.tokenizer.next() {
      match token {
        Token::String(name) => select.push(Expression::Identifier(name)),
        Token::Asterisk => select.push(Expression::Identifier("*".to_string())),
        Token::Keyword(Keyword::FROM) => {
          if select.is_empty() {
            return Err(ParserError::NoColumnsSpecified);
          }
          break;
        }
        Token::Comma => {}
        _ => return Err(ParserError::UnexpectedToken),
      }
    }

    Ok(select)
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

  fn parse_where_clause(&mut self) -> Result<Option<Expression>, ParserError> {
    match self.tokenizer.peek() {
      Some(Token::Keyword(Keyword::WHERE)) => {
        self.tokenizer.next();

        let mut condition = self.parse_condition()?;

        while let Some(operator) = self.parse_logical_operator() {
          let right_condition = self.parse_condition()?;
          condition = Expression::BinaryExpression { left: Box::new(condition), operator, right: Box::new(right_condition) };
        }

        Ok(Some(condition))
      }
      _ => Ok(None),
    }
  }

  fn parse_literal(&self, token: Token) -> Result<Literal, ParserError> {
    match token {
      Token::String(val) => Ok(Literal::String(val)),
      Token::Number(num) => num.parse().map(Literal::Number).map_err(|_| ParserError::FailedToParseNumber),
      Token::Date(date) => date.parse().map(Literal::Date).map_err(|_| ParserError::FailedToParseDate),
      Token::Timestamp(date) => date.parse().map(Literal::Timestamp).map_err(|_| ParserError::FailedToParseTimestamp),
      Token::Boolean(val) => Ok(Literal::Boolean(val)),
      Token::Null => Ok(Literal::Null),
      _ => Err(ParserError::ExpectedValue),
    }
  }

  fn parse_condition(&mut self) -> Result<Expression, ParserError> {
    let left = match self.tokenizer.next() {
      Some(Token::String(name)) => Box::new(Expression::Identifier(name)),
      _ => return Err(ParserError::ExpectedIdentifier),
    };

    let operator = self.tokenizer.next().ok_or(ParserError::UnexpectedEndOfStream)?.to_operator()?;

    let right = match self.tokenizer.next() {
      Some(token) => self.parse_literal(token).map(|literal| Box::new(Expression::Literal(literal))),
      _ => return Err(ParserError::ExpectedValue),
    }?;

    Ok(Expression::BinaryExpression { left, operator, right })
  }

  fn match_keyword_to_operator(&mut self, keyword: Keyword, operator: Operator) -> Option<Operator> {
    if let Some(Token::Keyword(k)) = self.tokenizer.peek() {
      if *k == keyword {
        self.tokenizer.next();
        return Some(operator);
      }
    }

    None
  }

  fn parse_logical_operator(&mut self) -> Option<Operator> {
    self
      .match_keyword_to_operator(Keyword::AND, Operator::And)
      .or_else(|| self.match_keyword_to_operator(Keyword::OR, Operator::Or))
  }

  fn parse_limit_and_offset(&mut self) -> Result<(Option<Expression>, Option<Expression>), ParserError> {
    let mut limit = None;
    let mut offset = None;

    for _ in 0..2 {
      match self.tokenizer.peek() {
        Some(Token::Keyword(Keyword::LIMIT)) if limit.is_none() => {
          limit = self.parse_limit_or_offset(Keyword::LIMIT)?;
        }
        Some(Token::Keyword(Keyword::OFFSET)) if offset.is_none() => {
          offset = self.parse_limit_or_offset(Keyword::OFFSET)?;
        }
        _ => break,
      }
    }

    Ok((limit, offset))
  }

  fn parse_limit_or_offset(&mut self, keyword: Keyword) -> Result<Option<Expression>, ParserError> {
    match self.tokenizer.peek() {
      Some(Token::Keyword(k)) if *k == keyword => {
        self.tokenizer.next();
        match self.tokenizer.next() {
          Some(Token::Number(num)) => num
            .parse::<f64>()
            .map(|number| Some(Expression::Literal(Literal::Number(number))))
            .map_err(|_| ParserError::FailedToParseNumber),
          _ => Err(ParserError::ExpectedValue),
        }
      }
      _ => Ok(None),
    }
  }

  fn check_if_next_keyword_is(&mut self, keyword: Keyword) -> bool {
    match self.tokenizer.peek() {
      Some(Token::Keyword(k)) if *k == keyword => true,
      _ => false,
    }
  }

  fn parse_group_by(&mut self, select: Vec<Expression>) -> Result<Option<Vec<Expression>>, ParserError> {
    let mut group_by_exprs = Vec::new();

    if self.check_if_next_keyword_is(Keyword::GROUP) {
      self.tokenizer.next();
      self.check_if_next_token_is_keyword(Keyword::BY)?;

      loop {
        let expr = self.parse_identifier_expression()?;

        if !select.contains(&expr) {
          return Err(ParserError::UnexpectedToken); // should be in select and if not throw that kind of erro
        }

        group_by_exprs.push(expr);

        if !self.peek_check_if_next_token_is(Token::Comma) {
          break;
        }
      }
    }

    Ok(if group_by_exprs.is_empty() { None } else { Some(group_by_exprs) })
  }

  fn parse_having(&mut self) -> Result<Option<Expression>, ParserError> {
    if self.peek_check_if_next_token_is(Token::Keyword(Keyword::HAVING)) {
      let mut condition = self.parse_condition()?;

      while let Some(operator) = self.parse_logical_operator() {
        let right_condition = self.parse_condition()?;
        condition = Expression::BinaryExpression { left: Box::new(condition), operator, right: Box::new(right_condition) };
      }

      Ok(Some(condition))
    } else {
      Ok(None)
    }
  }

  fn parse_order_direction(&mut self) -> Result<ast::Order, ParserError> {
    match self.tokenizer.peek() {
      Some(Token::Keyword(Keyword::ASC)) => {
        self.tokenizer.next();
        Ok(ast::Order::Asc)
      }
      Some(Token::Keyword(Keyword::DESC)) => {
        self.tokenizer.next();
        Ok(ast::Order::Desc)
      }
      _ => {
        self.tokenizer.next();
        Ok(ast::Order::Asc)
      }
    }
  }

  fn check_if_next_token_is_keyword(&mut self, keyword: Keyword) -> Result<(), ParserError> {
    match self.tokenizer.next() {
      Some(Token::Keyword(k)) if k == keyword => Ok(()),
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn peek_check_if_next_token_is(&mut self, expected_token: Token) -> bool {
    match self.tokenizer.peek() {
      Some(token) if *token == expected_token => {
        self.tokenizer.next();
        true
      }
      _ => false,
    }
  }

  fn parse_identifier_expression(&mut self) -> Result<Expression, ParserError> {
    match self.tokenizer.next() {
      Some(Token::String(name)) => Ok(Expression::Identifier(name)),
      _ => Err(ParserError::ExpectedIdentifier),
    }
  }

  fn parse_order_by(&mut self) -> Result<Option<Vec<(Expression, ast::Order)>>, ParserError> {
    let mut order_by_exprs = Vec::new();

    if let Some(Token::Keyword(Keyword::ORDER)) = self.tokenizer.peek() {
      self.tokenizer.next();

      self.check_if_next_token_is_keyword(Keyword::BY)?;
      loop {
        let expr = self.parse_identifier_expression()?;
        let order = self.parse_order_direction()?;

        order_by_exprs.push((expr, order));

        if !self.peek_check_if_next_token_is(Token::Comma) {
          break;
        }
      }
    }

    Ok(if order_by_exprs.is_empty() { None } else { Some(order_by_exprs) })
  }

  fn parse_select_statement(&mut self) -> Result<ast::Statement, ParserError> {
    let keyword = self.tokenizer.next();

    match keyword {
      Some(Token::Keyword(Keyword::SELECT)) => {
        let select = self.parse_select_columns()?;
        let from = self.parse_table()?;
        let where_clause = self.parse_where_clause()?;
        let group_by = self.parse_group_by(select.clone())?; // clone to check if group by is in select, maybe there is a better way to do this
        let having = self.parse_having()?;
        let order_by = self.parse_order_by()?;
        let (limit, offset) = self.parse_limit_and_offset()?;

        Ok(ast::Statement::Select(ast::SelectStatement { select, from, where_clause, group_by, having, order_by, limit, offset }))
      }
      Some(Token::Keyword(Keyword::DELETE)) => {
        self.check_if_next_token_is_keyword(Keyword::FROM)?;

        let from = self.parse_table()?;
        let where_clause = self.parse_where_clause()?;

        Ok(ast::Statement::Delete(ast::DeleteStatement { table: from, where_clause }))
      }
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn check_if_next_token_is(&mut self, expected_token: Token) -> Result<(), ParserError> {
    match self.tokenizer.next() {
      Some(token) if token == expected_token => Ok(()),
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_entries(&mut self) -> Result<Vec<(Expression, Expression)>, ParserError> {
    let mut entries = Vec::new();
    self.peek_check_if_next_token_is(Token::OpenParen);

    let columns = self.parse_intos()?;
    let values = self.parse_values()?;

    for i in 0..columns.len() {
      entries.push((columns[i].clone(), values[i].clone()));
    }

    Ok(entries)
  }

  fn parse_values(&mut self) -> Result<Vec<Expression>, ParserError> {
    let mut values = Vec::new();

    self.check_if_next_token_is_keyword(Keyword::VAULES)?;
    self.check_if_next_token_is(Token::OpenParen)?;

    loop {
      let token = self.tokenizer.next().ok_or(ParserError::UnexpectedEndOfStream)?;
      let literal = self.parse_literal(token)?;
      values.push(Expression::Literal(literal));

      if !self.peek_check_if_next_token_is(Token::Comma) {
        break;
      }
    }

    self.check_if_next_token_is(Token::CloseParen)?;

    Ok(values)
  }

  fn parse_intos(&mut self) -> Result<Vec<Expression>, ParserError> {
    let mut intos = Vec::new();

    loop {
      let token = self.parse_identifier_expression()?;
      intos.push(token);

      if !self.peek_check_if_next_token_is(Token::Comma) {
        break;
      }
    }

    self.check_if_next_token_is(Token::CloseParen)?;

    Ok(intos)
  }

  fn parse_set(&mut self) -> Result<Vec<(Expression, Expression)>, ParserError> {
    let mut entries = Vec::new();

    self.check_if_next_token_is_keyword(Keyword::SET)?;

    loop {
      let column = self.parse_identifier_expression()?;
      self.check_if_next_token_is(Token::Equal)?;

      let token = self.tokenizer.next().ok_or(ParserError::UnexpectedEndOfStream)?;
      let literal = self.parse_literal(token)?;

      entries.push((column, Expression::Literal(literal)));

      if !self.peek_check_if_next_token_is(Token::Comma) {
        break;
      }
    }

    Ok(entries)
  }

  fn parse_dml_statement(&mut self) -> Result<ast::Statement, ParserError> {
    let keyword = self.tokenizer.next();

    match keyword {
      Some(Token::Keyword(Keyword::INSERT)) => {
        self.check_if_next_token_is_keyword(Keyword::INTO)?;

        let table = self.parse_table()?;
        let entries = self.parse_entries()?;

        Ok(ast::Statement::Insert(ast::InsertStatement { table, entries }))
      }
      Some(Token::Keyword(Keyword::UPDATE)) => {
        let table = self.parse_table()?;
        let entries = self.parse_set()?;
        let where_clause = self.parse_where_clause()?;

        Ok(ast::Statement::Update(ast::UpdateStatement { table, entries, where_clause }))
      }
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_create_table_statement(&mut self) -> Result<ast::CreateTableStatement, ParserError> {
    let name = self.parse_identifier_expression()?;
    self.check_if_next_token_is(Token::OpenParen)?;

    let mut columns = Vec::new();
    loop {
      let column = self.parse_column_definition()?;
      columns.push(column);

      if !self.peek_check_if_next_token_is(Token::Comma) {
        break;
      }
    }

    self.check_if_next_token_is(Token::CloseParen)?;

    Ok(ast::CreateTableStatement { name, columns })
  }

  fn parse_column_definition(&mut self) -> Result<ast::ColumnDefinition, ParserError> {
    let name = self.parse_identifier_expression()?;
    let data_type = self.parse_data_type()?;

    let mut constraints = Vec::new();
    loop {
      if let Some(constraint) = self.parse_column_constraint()? {
        constraints.push(constraint);
      } else {
        break;
      }
    }

    Ok(ast::ColumnDefinition { name, data_type, constraints })
  }

  fn parse_column_constraint(&mut self) -> Result<Option<ast::ColumnConstraint>, ParserError> {
    match self.tokenizer.peek() {
      Some(Token::Keyword(Keyword::PRIMARY)) => {
        self.tokenizer.next();
        self.check_if_next_token_is_keyword(Keyword::KEY)?;

        Ok(Some(ast::ColumnConstraint::PrimaryKey))
      }
      Some(Token::Keyword(Keyword::NOT)) => {
        self.tokenizer.next();
        self.check_if_next_token_is(Token::Null)?;

        Ok(Some(ast::ColumnConstraint::NotNull))
      }
      Some(Token::Keyword(Keyword::UNIQUE)) => {
        self.tokenizer.next();

        Ok(Some(ast::ColumnConstraint::Unique))
      }
      Some(Token::Keyword(Keyword::CHECK)) => {
        self.tokenizer.next();
        let condition = self.parse_condition()?;

        Ok(Some(ast::ColumnConstraint::Check(condition)))
      }
      Some(Token::Keyword(Keyword::FOREIGN)) => {
        self.tokenizer.next();
        self.check_if_next_token_is_keyword(Keyword::KEY)?;
        self.check_if_next_token_is(Token::OpenParen)?;

        let child_column = self.parse_identifier_expression()?;

        self.check_if_next_token_is(Token::CloseParen)?;
        self.check_if_next_token_is_keyword(Keyword::REFERENCES)?;

        let parent_table = self.parse_identifier_expression()?;

        self.check_if_next_token_is(Token::OpenParen)?;

        let parent_column = self.parse_identifier_expression()?;
        self.check_if_next_token_is(Token::CloseParen)?;

        Ok(Some(ast::ColumnConstraint::ForeignKey { table: parent_table, child_column, parent_column }))
      }
      _ => Ok(None),
    }
  }

  fn parse_data_type(&mut self) -> Result<ast::DataType, ParserError> {
    match self.tokenizer.next() {
      Some(Token::Keyword(Keyword::INT)) => Ok(ast::DataType::Int),
      Some(Token::Keyword(Keyword::TEXT)) => Ok(ast::DataType::Text),
      Some(Token::Keyword(Keyword::DATE)) => Ok(ast::DataType::Date),
      Some(Token::Keyword(Keyword::TIMESTAMP)) => Ok(ast::DataType::Timestamp),
      Some(Token::Keyword(Keyword::BOOLEAN)) => Ok(ast::DataType::Boolean),
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_alter_table_operation(&mut self) -> Result<ast::AlterTableOperation, ParserError> {
    match self.tokenizer.next() {
      Some(Token::Keyword(Keyword::ADD)) => {
        self.check_if_next_token_is_keyword(Keyword::COLUMN)?;
        let column = self.parse_column_definition()?;

        Ok(ast::AlterTableOperation::AddColumn(column))
      }
      Some(Token::Keyword(Keyword::DROP)) => {
        self.check_if_next_token_is_keyword(Keyword::COLUMN)?;
        let column = self.parse_identifier_expression()?;

        Ok(ast::AlterTableOperation::DropColumn(column))
      }
      Some(Token::Keyword(Keyword::MODIFY)) => {
        self.check_if_next_token_is_keyword(Keyword::COLUMN)?;
        let column = self.parse_column_definition()?;

        Ok(ast::AlterTableOperation::ModifyColumn(column))
      }
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_ddl_statement(&mut self) -> Result<ast::Statement, ParserError> {
    let keyword = self.tokenizer.next();
    self.check_if_next_token_is_keyword(Keyword::TABLE)?;

    match keyword {
      Some(Token::Keyword(Keyword::CREATE)) => {
        let statement = self.parse_create_table_statement()?;
        Ok(ast::Statement::CreateTable(statement))
      }
      Some(Token::Keyword(Keyword::DROP)) => {
        let name = self.parse_identifier_expression()?;

        Ok(ast::Statement::DropTable(ast::DropTableStatement { name }))
      }
      Some(Token::Keyword(Keyword::ALTER)) => {
        let name = self.parse_identifier_expression()?;
        let operation = self.parse_alter_table_operation()?;

        Ok(ast::Statement::AlterTable(ast::AlterTableStatement { name, operation }))
      }
      _ => Err(ParserError::UnexpectedToken),
    }
  }

  fn parse_transaction(&mut self) -> Result<ast::Statement, ParserError> {
    let keyword = self.tokenizer.next();

    match keyword {
      Some(Token::Keyword(Keyword::BEGIN)) => {
        self.check_if_next_token_is_keyword(Keyword::TRANSACTION)?;
        Ok(ast::Statement::Begin)
      }
      Some(Token::Keyword(Keyword::COMMIT)) => Ok(ast::Statement::Commit),
      Some(Token::Keyword(Keyword::ROLLBACK)) => Ok(ast::Statement::Rollback),
      _ => Err(ParserError::UnexpectedToken),
    }
  }
}
