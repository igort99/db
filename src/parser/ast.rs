#[derive(Debug)]
pub enum Statement {
  Select(SelectStatement),
  Insert(InsertStatement),
  Update(UpdateStatement),
  Delete(DeleteStatement),
  Transaction(Vec<Statement>),
}
#[derive(Debug)]
pub struct SelectStatement {
  pub from: Table,
  pub select: Vec<String>,
  pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct Table {
  pub name: String,
  pub alias: Option<String>,
}

#[derive(Debug)]
pub struct InsertStatement {
  pub table: String,
  pub columns: Vec<String>,
  pub values: Vec<Expression>,
}

#[derive(Debug)]
pub struct UpdateStatement {
  pub table: String,
  pub assignments: Vec<(String, Expression)>,
  pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct DeleteStatement {
  pub table: String,
  pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub enum Expression {
  Literal(Literal),
  Identifier(String),
  BinaryExpression { left: Box<Expression>, operator: Operator, right: Box<Expression> },
  AndConditions(Vec<Expression>),
}

#[derive(Debug)]
pub enum Literal {
  String(String),
  Number(f64),
  Boolean(bool),
}

#[derive(Debug)]
pub enum Operator {
  Add,
  Subtract,
  Multiply,
  Divide,
  Equal,
  NotEqual,
  LessThan,
  LessThanOrEqual,
  GreaterThan,
  GreaterThanOrEqual,
}
