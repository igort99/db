#![allow(unused)]
pub mod sql;
pub mod storage;
// fn main() {
//   // let input = "SELECT age, name FROM table WHERE adult = 'yes' and
//   // year = '2012-01-22' and is_emigrant = true and name = null
//   // group by name, age
//   // having age > 18
//   // order by name asc, age desc
//   // offset 6 limit 5";
//   // let input = "DELETE FROM users WHERE age > 18 and name = 'John' and is_emigrant = true and name = null";

//   // let input = "INSERT INTO users (name, age, is_emigrant) VALUES ('John', 25, true)";

//   // let input = "UPDATE users SET name = 'John', age = 25, is_emigrant = true WHERE id = 1";

//   // let input = "CREATE TABLE users (uuid INT PRIMARY KEY, name TEXT NOT NULL, age INT, is_emigrant BOOLEAN)";
//   // one with foereign key
//   // let input = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, is_emigrant BOOLEAN,
//   // country_id INT FOREIGN KEY (country_id) REFERENCES countries (id))";
//   // let input = "DROP TABLE users";
//   // let input = "ALTER TABLE users ADD COLUMN age INT";
//   // let input = "ALTER TABLE users DROP COLUMN age";
//   let input = "ALTER TABLE users MODIFY COLUMN age INT";

//   let mut parser = parser::Parser::new(input);
//   let statement = parser.parse();
//   println!("{:?}", statement);
// }

use std::io::{self, Write};

use crate::sql::{
  catalog,
  planner::plan::{Node, Plan},
};

fn main() {
  let mut input = String::new();

  loop {
    print!("Enter SQL query: ");
    io::stdout().flush().unwrap();

    input.clear();
    io::stdin().read_line(&mut input).unwrap();

    if input.trim() == "exit" {
      break;
    }

    let mut storage_manager = storage::manager::StorageManager::new();
    let mut buffer_pool = storage::manager::BufferPool::new();
    let mut catalog = buffer_pool.get_catalog();

    let mut parser = sql::parser::Parser::new(&input);
    let statement = parser.parse();

    let plan = sql::planner::plan::Planner::new().build(statement.unwrap());
    println!("{:?}", plan);
    let mut optimizer = sql::optimizer::optimizer::Optimizer::new(plan, catalog);
    let physical_plan = optimizer.optimize();
    println!("{:?}", physical_plan);
    // println!("{:?}", catalog);

    match &physical_plan.0 {
      Node::CreateTable { schema } => {
        let table = sql::catalog::catalog::Table::new(schema.name.clone(), schema.columns.clone());
        buffer_pool.add_table_to_catalog(table);
      }
      // Node::DropTable { table } => {
      //   buffer_pool.remove_table_from_catalog(table);
      // }
      _ => {}
    }
  }
}
