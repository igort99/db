use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Catalog {
  pub tables: HashMap<String, Table>,
  pub users: Option<Vec<String>>, // INFO: Only string for now so I can decide later do I want to implement it
  pub views: Option<Vec<String>>, // INFO: Only string for now so I can decide later do I want to implement it
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Table {
  pub name: String,
  pub columns: HashMap<String, Column>,
}

impl Table {
  pub fn new(name: String, columns: HashMap<String, Column>) -> Self {
    Self { name, columns }
  }

  pub fn add_column(&mut self, column: Column) {
    self.columns.insert(column.name.clone(), column);
  }

  pub fn remove_column(&mut self, column_name: &str) -> Option<Column> {
    self.columns.remove(column_name)
  }

  pub fn get_column(&self, column_name: &str) -> Option<&Column> {
    self.columns.get(column_name)
  }

  pub fn get_mut_column(&mut self, column_name: &str) -> Option<&mut Column> {
    self.columns.get_mut(column_name)
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
  pub name: String,
  pub data_type: DataType,
  pub unique: bool,
  pub nullable: bool,
  pub default: Option<String>,
  pub references: Option<Reference>,
}

impl Column {
  pub fn update_name(&mut self, new_name: String) {
    self.name = new_name
  }

  pub fn update_data_type(&mut self, new_data_type: DataType) {
    self.data_type = new_data_type;
  }

  pub fn update_unique(&mut self, new_unique: bool) {
    self.unique = new_unique;
  }

  pub fn update_nullable(&mut self, new_nullable: bool) {
    self.nullable = new_nullable;
  }

  pub fn update_default(&mut self, new_default: Option<String>) {
    self.default = new_default;
  }

  pub fn update_references(&mut self, new_references: Option<Reference>) {
    self.references = new_references;
  }

  pub fn get_attribute(&self, attribute_name: &str) -> Option<&dyn std::fmt::Debug> {
    match attribute_name {
      "name" => Some(&self.name),
      "data_type" => Some(&self.data_type),
      "unique" => Some(&self.unique),
      "nullable" => Some(&self.nullable),
      "default" => self.default.as_ref().map(|v| v as &dyn std::fmt::Debug),
      "references" => self.references.as_ref().map(|v| v as &dyn std::fmt::Debug),
      _ => None,
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reference {
  pub table_name: i32,
  pub column_name: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
  Int,
  Text,
  Boolean,
  Float,
  Date,
  DateTime,
  Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
  Int(i32),
  Float(f64),
  Text(String),
  Boolean(bool),
}

impl Catalog {
  pub fn new(tables: HashMap<String, Table>) -> Self {
    Self { tables, users: None, views: None }
  }

  pub fn add_table(&mut self, table: Table) {
    self.tables.insert(table.name.clone(), table);
  }

  pub fn get_table(&self, table_name: &str) -> Option<&Table> {
    self.tables.get(table_name)
  }

  pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
    self.tables.get_mut(table_name)
  }

  pub fn remove_table(&mut self, table_name: &str) -> Option<Table> {
    self.tables.remove(table_name)
  }

  pub fn get_column(&self, table_name: &str, column_name: &str) -> Option<&Column> {
    self.tables.get(table_name).and_then(|table| table.get_column(column_name))
  }

  pub fn add_column(&mut self, table_name: &str, column: Column) {
    self.tables.get_mut(table_name).map(|table| table.add_column(column));
  }

  pub fn update_column(&mut self, updated_column: Column, column_name: &str, table_name: &str) {
    if let Some(column) = self.tables.get_mut(table_name).and_then(|table| table.get_mut_column(column_name)) {
      *column = updated_column
    }
  }

  pub fn update_table(&mut self, table_name: &str, updated_table: Table) {
    if let Some(table) = self.tables.get_mut(table_name) {
      *table = updated_table
    }
  }

  pub fn remove_column(&mut self, table_name: &str, column_name: &str) {
    self.tables.get_mut(table_name).map(|table| table.remove_column(column_name));
  }
}
