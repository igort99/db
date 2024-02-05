pub mod parser;
fn main() {
  let input = "SELECT * FROM table WHERE age = 22";
  // let mut tokenizer = Tokenizer::new(input);
  // let token = tokenizer.read();
  // println!("{:?}", token);
  // let token2 = tokenizer.read();
  // println!("{:?}", token2);
  // let token3 = tokenizer.read();
  // println!("{:?}", token3);
  // let token4 = tokenizer.read();
  // println!("{:?}", token4);
  // let token5 = tokenizer.read();
  // println!("{:?}", token5);
  // let token6 = tokenizer.read();
  // println!("{:?}", token6);
  // let token7 = tokenizer.read();
  // println!("{:?}", token7);
  // let token8 = tokenizer.read();
  // println!("{:?}", token8);

  let mut parser = parser::Parser::new(input);
  let statement = parser.parse();
  println!("{:?}", statement);
}
