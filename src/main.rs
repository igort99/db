pub mod parser;
fn main() {
  let input = "SELECT age, name FROM table WHERE adult = 'yes' and 
  year = '2012-01-22' and is_emigrant = true and name = null 
  group by name, age
  having age > 18
  order by name asc, age desc
  offset 6 limit 5";

  let mut parser = parser::Parser::new(input);
  let statement = parser.parse();
  println!("{:?}", statement);
}
