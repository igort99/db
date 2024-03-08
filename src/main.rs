pub mod sql;
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

//   // let input = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, is_emigrant BOOLEAN)";
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

fn main() {
    let mut input = String::new();

    loop {
        print!("Enter SQL query: ");
        io::stdout().flush().unwrap(); // Make sure the prompt is immediately displayed

        input.clear(); // Clear the buffer
        io::stdin().read_line(&mut input).unwrap(); // Read a line into the buffer

        if input.trim() == "exit" { // If the user types "exit", break the loop
            break;
        }

        let mut parser = sql::parser::Parser::new(&input);
        let statement = parser.parse();
        
        // match statement {
        //     Ok(statement) => println!("{:?}", statement),
        //     Err(e) => println!("Error: {}", e),  
        // }

        let plan = sql::planner::plan::Planner::new().build(statement.unwrap());

        println!("{:?}", plan);
    }
}
