use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

#[derive(Debug)]
pub enum Statement {
  // INFO: Transaction control
  Begin,
  Commit,
  Rollback,

  // INFO: Query
  Select {
    from: Table,
    select: Vec<Expression>,
    where_clause: Option<Expression>,
    group_by: Option<Vec<Expression>>,
    having: Option<Expression>,
    order_by: Option<Vec<(Expression, Order)>>,
    limit: Option<Expression>,
    offset: Option<Expression>,
  },

  // INFO: DMLs
  Insert {
    table: Table,
    entries: Vec<(Expression, Expression)>,
  },
  Update {
    table: Table,
    entries: Vec<(Expression, Expression)>,
    where_clause: Option<Expression>,
  },
  Delete {
    table: Table,
    where_clause: Option<Expression>,
  },

  // INFO: DDLs
  CreateTable {
    name: Expression,
    columns: Vec<ColumnDefinition>,
  },
  DropTable {
    name: Expression,
  },
  AlterTable {
    name: Expression,
    operation: AlterTableOperation,
  },
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
  pub name: Expression,
  pub data_type: DataType,
  pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone)]
pub enum ColumnConstraint {
  PrimaryKey,
  NotNull,
  Unique,
  Default(Expression),
  Check(Expression),
  ForeignKey { table: Expression, child_column: Expression, parent_column: Expression },
}

#[derive(Debug, Clone)]
pub enum DataType {
  Int,
  Text,
  Date,
  Timestamp,
  Boolean,
}

#[derive(Debug)]
pub enum AlterTableOperation {
  AddColumn(ColumnDefinition),
  DropColumn(Expression),
  ModifyColumn(ColumnDefinition),
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