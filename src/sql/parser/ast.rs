use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

#[derive(Debug)]
pub enum Statement {
  Begin,
  Commit,
  Rollback,
  Select(SelectStatement),
  Insert(InsertStatement),
  Update(UpdateStatement),
  Delete(DeleteStatement),
  CreateTable(CreateTableStatement),
  DropTable(DropTableStatement),
  AlterTable(AlterTableStatement),
}

#[derive(Debug)]
pub struct CreateTableStatement {
  pub name: Expression,
  pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug)]
pub struct ColumnDefinition {
  pub name: Expression,
  pub data_type: DataType,
  pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug)]
pub enum DataType {
  Int,
  Text,
  Date,
  Timestamp,
  Boolean,
}

#[derive(Debug)]
pub enum ColumnConstraint {
  PrimaryKey,
  NotNull,
  Unique,
  Default(Expression),
  Check(Expression),
  ForeignKey { table: Expression, child_column: Expression, parent_column: Expression },
}

#[derive(Debug)]
pub struct DropTableStatement {
  pub name: Expression,
}

#[derive(Debug)]
pub struct AlterTableStatement {
  pub name: Expression,
  pub operation: AlterTableOperation,
}

#[derive(Debug)]
pub enum AlterTableOperation {
  AddColumn(ColumnDefinition),
  DropColumn(Expression),
  ModifyColumn(ColumnDefinition),
}

#[derive(Debug)]
pub struct SelectStatement {
  pub from: Table,
  pub select: Vec<Expression>,
  pub where_clause: Option<Expression>,
  pub group_by: Option<Vec<Expression>>,
  pub having: Option<Expression>,
  pub order_by: Option<Vec<(Expression, Order)>>,
  pub limit: Option<Expression>,
  pub offset: Option<Expression>,
}

#[derive(Debug)]
pub enum Order {
  Asc,
  Desc,
}

#[derive(Debug)]
pub struct Table {
  pub name: String,
  pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
  Literal(Literal),
  Identifier(String),
  BinaryExpression { left: Box<Expression>, operator: Operator, right: Box<Expression> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
  String(String),
  Number(f64),
  Boolean(bool),
  Null,
  Date(NaiveDate),
  DateTime(NaiveDateTime),
  Timestamp(DateTime<Utc>),
}

#[derive(Debug, Clone, PartialEq)]
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
  And,
  Or,
  Asterisk,
}

#[derive(Debug)]
pub struct InsertStatement {
  pub table: Table,
  pub entries: Vec<(Expression, Expression)>,
}

#[derive(Debug)]
pub struct UpdateStatement {
  pub table: Table,
  pub entries: Vec<(Expression, Expression)>,
  pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct DeleteStatement {
  pub table: Table,
  pub where_clause: Option<Expression>,
}
