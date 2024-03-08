// TODO: Should accept AST and plan operations. So output will be execution plan which will be input for optimizer
//       Planer needs to decide based on AST Statement what it needs to do with the table
//       It needs to decide if it needs to create table, insert, update, delete, select, alter table, drop table
//       But everything plays around get/update/create data and schema, so everything is encapsulated in schema

use crate::sql::{
  parser::ast::{self, CreateTableStatement, SelectStatement},
  schema::{Column, DataType, Table},
};

#[derive(Debug)]
pub struct Plan(pub Node);

#[derive(Debug)]
pub enum Node {
  CreateTable { schema: Table },
}

#[derive(Debug)]
pub struct Planner {
  // INFO: This is the schema of the database
  // catalog: &'a  // should be catalog
}

impl Planner {
  pub fn new() -> Self {
    Planner {}
  }

  pub fn build(&mut self, statement: ast::Statement) -> Plan {
    Plan(self.bind(statement))
  }

  fn bind(&mut self, statement: ast::Statement) -> Node {
    match statement {
      ast::Statement::CreateTable(create_table_statement) => {
        let CreateTableStatement { name, columns } = create_table_statement;
        let columns = columns.into_iter().map(column_definition_to_column).collect();
        let name = parse_identifier(name);

        Node::CreateTable { schema: Table::new(name, columns) }
      }
      ast::Statement::Select(select_statement) => {
        let SelectStatement { from, select, where_clause, group_by, having, order_by, limit, offset } = select_statement;
      }
      _ => unimplemented!(),
    }
  }
}

fn parse_identifier(expr: ast::Expression) -> String {
  match expr {
    ast::Expression::Identifier(name) => name,
    _ => unimplemented!(),
  }
}

fn data_type_to_primitive(data_type: ast::DataType) -> DataType {
  match data_type {
    ast::DataType::Int => crate::sql::schema::DataType::Int,
    ast::DataType::Text => crate::sql::schema::DataType::Text,
    ast::DataType::Boolean => crate::sql::schema::DataType::Boolean,
    _ => unimplemented!(),
  }
}

fn literal_to_value(literal: ast::Literal) -> Option<crate::sql::schema::Value> {
  match literal {
    ast::Literal::Number(value) => Some(crate::sql::schema::Value::Float(value)),
    ast::Literal::String(value) => Some(crate::sql::schema::Value::Text(value)),
    ast::Literal::Boolean(value) => Some(crate::sql::schema::Value::Boolean(value)),
    _ => None,
  }
}

pub fn column_definition_to_column(column_definition: ast::ColumnDefinition) -> Column {
  let ast::ColumnDefinition { name, data_type, constraints } = column_definition;

  Column {
    id: 0,                        // i need id checkups and autoincrement
    name: parse_identifier(name), // check if it is unique on the table and if not throw error
    data_type: data_type_to_primitive(data_type),
    value: None,
    unqiue: false,
    nullable: false,
    default: None,
    references: None,
  }
}

pub fn get_column_by_name<'a>(table: &'a Table, name: &'a str) -> Option<&'a Column> {
  table.columns.iter().find(|column| column.name == name)
}
