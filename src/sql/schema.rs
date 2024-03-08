use super::parser::ast::Table as TableAst;

// TODO: So when we set create table query we need to create table in schema/catalog
// After creating table we need methods for get/insert/update/delete for tables and columns
// We need functions that decide based od AST what to do with the table
// Here we should have CRUD operations for table and in planner we should have deciding process

fn create_table(table_ast: TableAst) -> () {
    
}

#[derive(Debug)]
pub struct Table {
    // INFO: Unique id for the table used for references and for internal use
    pub id: i32,

    pub name: String,
    // TODO: Add alias maybe

    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Table {
        Table {
            id: 0, // i need id checkups and autoincrement
            name,
            columns,
        }
    }
}

#[derive(Debug)]
pub struct Column {
    // INFO: Unique id for the column used for references and for internal use
    pub id: i32,

    pub name: String,
    pub data_type: DataType,
    pub value: Option<Value>,
    pub unqiue: bool,
    pub nullable: bool,
    pub default: Option<String>,
    pub references: Option<Reference>,
}

#[derive(Debug)]
pub struct Reference {
    pub table_id: i32,
    pub column_id: i32,
}

#[derive(Debug)]
pub enum DataType {
    Int,
    Text,
    Boolean,
}

#[derive(Debug)]
pub enum Value {
    Int(i32),
    Float(f64),
    Text(String),
    Boolean(bool),
}