use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Catalog {
  pub tables: HashMap<String, Table>,
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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
  pub name: String,
  pub data_type: DataType,
  pub value: Option<Value>,
  pub unqiue: bool,
  pub nullable: bool,
  pub default: Option<String>,
  pub references: Option<Reference>,
  // indexes: Vec<Index>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reference {
  pub table_id: i32,
  pub column_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
  Int,
  Text,
  Boolean,
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
    Self { tables }
  }

  pub fn add_table(&mut self, table: Table) {
    let table_name = table.name.clone();
    self.tables.insert(table_name, table);
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
    self.tables.get(table_name).and_then(|table| table.columns.get(column_name))
  }
}
