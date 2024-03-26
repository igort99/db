#![allow(unused)]
use std::vec;

use crate::sql::{
  catalog::{Column, DataType, Table},
  parser::ast::{self, AlterTableOperation, Statement},
};

#[derive(Debug)]
pub struct Plan(pub Node);

#[derive(Debug, Clone)]
pub enum Node {
  // INFO: DDl sts
  CreateTable { schema: Table },
  DropTable { table: String },
  AlterTable { table: String, values: Vec<(Expression, Expression)> },

  // INFO: Insert sts
  Insert { table: String, values: Vec<(Expression, Expression)> },
  Update { table: String, values: Vec<(Expression, Expression)> },
  Delete { table: String },

  // INFO: Select sts
  Limit { source: Box<Node>, limit: Expression },
  Offset { source: Box<Node>, offset: Expression },
  Projection { source: Box<Node>, columns: Vec<Expression> },
  Filter { source: Box<Node>, condition: Expression },
  GroupBy { source: Box<Node>, values: Vec<Expression> },
  Having { source: Box<Node>, condition: Expression }, // INFO: Maybe this can go to filter

  Scan { table: String, alias: Option<String>, filter: Option<Expression> },

  // TODO: Implement
  IndexLookup { table: String, alias: Option<String>, index: String },
  NestedLoopJoin { left: Box<Node>, right: Box<Node>, condition: Expression },
  HashJoin { left: Box<Node>, right: Box<Node>, condition: Expression },
  Sort { source: Box<Node>, order: Vec<(Expression, bool)> },
}

impl Node {
  pub fn get_table(&self) -> Option<&String> {
    match self {
      Node::CreateTable { schema } => Some(&schema.name),
      Node::DropTable { table } => Some(table),
      Node::AlterTable { table, .. } => Some(table),
      Node::Insert { table, .. } => Some(table),
      Node::Update { table, .. } => Some(table),
      Node::Delete { table } => Some(table),
      Node::Scan { table, .. } => Some(table),
      _ => None,
    }
  }

  pub fn tranverse(&self) -> Vec<&Node> {
    let mut nodes = vec![self];

    match self {
      Node::CreateTable { .. }
      | Node::DropTable { .. }
      | Node::AlterTable { .. }
      | Node::Insert { .. }
      | Node::Update { .. }
      | Node::Delete { .. }
      | Node::Scan { .. } => nodes,
      Node::Limit { source, .. }
      | Node::Offset { source, .. }
      | Node::Projection { source, .. }
      | Node::Filter { source, .. }
      | Node::GroupBy { source, .. }
      | Node::Having { source, .. } => {
        nodes.push(source);
        nodes
      }
      _ => unimplemented!(),
    }
  }
}

#[derive(Debug, Clone)]
pub enum Expression {
  Identifier(String),
  Constant(Value),
  DataType(DataType),

  Equal(Box<Expression>, Box<Expression>),
  NotEqual(Box<Expression>, Box<Expression>),
  GreaterThan(Box<Expression>, Box<Expression>),
  GreaterThanOrEqual(Box<Expression>, Box<Expression>),

  Add(Box<Expression>, Box<Expression>),
  Subtract(Box<Expression>, Box<Expression>),
  Multiply(Box<Expression>, Box<Expression>),
  Divide(Box<Expression>, Box<Expression>),

  And(Box<Expression>, Box<Expression>),
  Or(Box<Expression>, Box<Expression>),
}

#[derive(Debug, Clone)]
pub enum Value {
  Int(i64),
  Float(f64),
  Text(String),
  Boolean(bool),
  Null,
}

#[derive(Debug)]
pub struct Planner {}

impl Planner {
  pub fn new() -> Self {
    Planner {}
  }

  pub fn build(&mut self, statement: ast::Statement) -> Plan {
    Plan(self.bind(statement))
  }

  fn bind(&mut self, statement: ast::Statement) -> Node {
    match statement {
      ast::Statement::Begin { .. } | ast::Statement::Commit | ast::Statement::Rollback => {
        panic!("Transaction not supported yet in this point in code")
      }
      ast::Statement::CreateTable { name, columns } => {
        let columns = columns
          .into_iter()
          .map(|column| {
            let name = column.name.clone().parse_identifier();
            (name, column_definition_to_column(column))
          })
          .collect();

        let name = name.parse_identifier();

        Node::CreateTable { schema: Table::new(name, columns) }
      }
      ast::Statement::DropTable { name } => Node::DropTable { table: name.parse_identifier() },
      ast::Statement::AlterTable { name, operation } => {
        let table = name.parse_identifier();
        let values = match operation {
          AlterTableOperation::AddColumn(column) => {
            let column_name = column.name.parse_identifier();
            let column_type = data_type_to_primitive(column.data_type);

            vec![(Expression::Identifier(column_name), Expression::DataType(column_type))]
          }
          AlterTableOperation::DropColumn(column) => {
            vec![(Expression::Identifier(column.parse_identifier()), Expression::Identifier("".to_string()))]
          }
          AlterTableOperation::ModifyColumn(column) => {
            let column_name = column.name.parse_identifier();
            let column_type = data_type_to_primitive(column.data_type);

            vec![(Expression::Identifier(column_name), Expression::DataType(column_type))]
          }
        };

        Node::AlterTable { table, values }
      }
      ast::Statement::Insert { table, entries } => {
        let values: Vec<(Expression, Expression)> =
          entries.into_iter().map(|(name, value)| (expr_to_expression(name), expr_to_expression(value))).collect();

        Node::Insert { table: table.name, values }
      }
      ast::Statement::Update { table, entries, where_clause } => {
        let values: Vec<(Expression, Expression)> =
          entries.into_iter().map(|(name, value)| (expr_to_expression(name), expr_to_expression(value))).collect();

        let mut node = Node::Update { table: table.name, values };

        if let Some(condition) = where_clause.map(expr_to_expression) {
          node = Node::Filter { source: Box::new(node), condition };
        }

        node
      }
      ast::Statement::Delete { table, where_clause } => {
        let mut node = Node::Delete { table: table.name };

        if let Some(condition) = where_clause.map(expr_to_expression) {
          node = Node::Filter { source: Box::new(node), condition };
        }

        node
      }
      ast::Statement::Select { from, select, where_clause, group_by, having, order_by, limit, offset } => {
        let mut node = Node::Scan { table: from.name, alias: from.alias, filter: None };

        if let Some(condition) = where_clause.map(expr_to_expression) {
          node = Node::Filter { source: Box::new(node), condition };
        } // add checks for joins and indexes

        if let Some(limit) = limit {
          node = Node::Limit { source: Box::new(node), limit: expr_to_expression(limit) };
        }

        if let Some(offset) = offset {
          node = Node::Offset { source: Box::new(node), offset: expr_to_expression(offset) };
        }

        node = Node::Projection { source: Box::new(node), columns: select.into_iter().map(expr_to_expression).collect() };

        node
      }
      _ => unimplemented!(),
    }
  }
}

fn expr_to_expression(expr: ast::Expression) -> Expression {
  match expr {
    ast::Expression::Identifier(name) => Expression::Identifier(name),
    ast::Expression::Literal(literal) => Expression::Constant(literal_to_value(literal).unwrap()),
    ast::Expression::BinaryExpression { left, operator, right } => binary_operator_to_expression(operator, *left, *right),
    _ => unimplemented!(),
  }
}

fn binary_operator_to_expression(operator: ast::Operator, left: ast::Expression, right: ast::Expression) -> Expression {
  match operator {
    ast::Operator::Equal => Expression::Equal(Box::new(expr_to_expression(left)), Box::new(expr_to_expression(right))),
    ast::Operator::NotEqual => Expression::NotEqual(Box::new(expr_to_expression(left)), Box::new(expr_to_expression(right))),
    ast::Operator::GreaterThan => {
      Expression::GreaterThan(Box::new(expr_to_expression(left)), Box::new(expr_to_expression(right)))
    }
    ast::Operator::GreaterThanOrEqual => {
      Expression::GreaterThanOrEqual(Box::new(expr_to_expression(left)), Box::new(expr_to_expression(right)))
    }
    ast::Operator::And => Expression::And(Box::new(expr_to_expression(left)), Box::new(expr_to_expression(right))),
    ast::Operator::Or => Expression::Or(Box::new(expr_to_expression(left)), Box::new(expr_to_expression(right))),
    _ => unimplemented!(),
  }
}

fn data_type_to_primitive(data_type: ast::DataType) -> DataType {
  match data_type {
    ast::DataType::Int => crate::sql::catalog::DataType::Int,
    ast::DataType::Text => crate::sql::catalog::DataType::Text,
    ast::DataType::Boolean => crate::sql::catalog::DataType::Boolean,
    _ => unimplemented!(),
  }
}

fn literal_to_value(literal: ast::Literal) -> Option<Value> {
  match literal {
    ast::Literal::Number(value) => Some(Value::Float(value)),
    ast::Literal::String(value) => Some(Value::Text(value)),
    ast::Literal::Boolean(value) => Some(Value::Boolean(value)),
    ast::Literal::Null => Some(Value::Null),
    _ => None,
  }
}

pub fn column_definition_to_column(column_definition: ast::ColumnDefinition) -> Column {
  let ast::ColumnDefinition { name, data_type, constraints } = column_definition;

  Column {
    name: name.parse_identifier(), // check if it is unique on the table and if not throw error
    data_type: data_type_to_primitive(data_type),
    unique: false,
    nullable: false,
    default: None,
    references: None,
  }
}

pub fn get_column_by_name<'a>(table: &'a Table, name: &'a str) -> Option<&'a Column> {
  table.columns.iter().find(|(column_name, _)| *column_name == name).map(|(_, column)| column)
}
