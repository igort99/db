#[derive(Debug)]
pub enum Statement {
  Select(SelectStatement),
  Insert(InsertStatement),
  Update(UpdateStatement),
  Delete(DeleteStatement),
  Transaction(Vec<Statement>),
}
#[derive(Debug)]
pub struct SelectStatement {
  pub from: Table,
  pub select: Vec<String>,
  pub where_clause: Option<Expression>,
  pub limit: Option<usize>,
  pub offset: Option<usize>,
}

#[derive(Debug)]
pub struct Table {
  pub name: String,
  pub alias: Option<String>,
}

#[derive(Debug)]
pub struct InsertStatement {
  pub table: String,
  pub columns: Vec<String>,
  pub values: Vec<Expression>,
}

#[derive(Debug)]
pub struct UpdateStatement {
  pub table: String,
  pub assignments: Vec<(String, Expression)>,
  pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct DeleteStatement {
  pub table: String,
  pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub enum Expression {
  Literal(Literal),
  Identifier(String),
  BinaryExpression { left: Box<Expression>, operator: Operator, right: Box<Expression> },
}

impl Expression {
  pub fn walk<F: Fn(&Expression) -> bool>(&self, visitor: &F) -> bool {
    match self {
      Expression::BinaryExpression { left, operator: _, right } => visitor(self) && left.walk(visitor) && right.walk(visitor),
      _ => visitor(self),
    }
  }

  pub fn transform<F: FnMut(Expression) -> Result<Expression, &'static str>>(&mut self, mut f: F) -> Result<(), &'static str> {
    let expr = std::mem::replace(self, Expression::Literal(Literal::Number(0.0)));
    *self = f(expr)?;
    Ok(())
  }

  pub fn transform_tree<F: FnMut(Expression) -> Result<Expression, &'static str>>(
    &mut self,
    mut f: F,
  ) -> Result<(), &'static str> {
    match self {
      Expression::BinaryExpression { left, operator: _, right } => {
        left.transform_tree(&mut f)?;
        right.transform_tree(&mut f)?;
      }
      _ => {}
    }
    self.transform(f)
  }
}

#[derive(Debug, Clone)]
pub enum Literal {
  String(String),
  Number(f64),
  Boolean(bool),
}

#[derive(Debug, Clone)]
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
}
