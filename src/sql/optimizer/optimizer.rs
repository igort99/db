#![allow(unused)]
use std::collections::{BTreeMap, HashMap};

use crate::sql::{
  catalog::{self, Catalog},
  planner::plan::{Expression, Node, Plan, Value},
};

#[derive(Debug)]
pub struct PhysicalPlan {
  pub node: Op,
  cost: Option<i32>,
  childern: Option<Vec<PhysicalPlan>>,
}

#[derive(Debug)]
pub enum Op {
  TableScan { data_source: String, alias: Option<String> }, //filter
  IndexScan { data_source: String, alias: Option<String>, index: String }, // filter
  Projection { columns: Vec<String> },

  CreateTable { table: catalog::Table },
  DropTable { table_name: String },

  Insert { data_source: String, rows: Vec<HashMap<String, catalog::Value>> },
}

pub(crate) struct Optimizer<'a> {
  pub plan: Plan,
  pub catalog: &'a mut Catalog,
}
/***********/
// treba da vrati fizicki plan ali da prte toga uradi optimizaciju
// predicate push down i projection push down
// treba da se implementira i cost based optimization
// treba da se implementira i join ordering

impl<'a> Optimizer<'a> {
  pub fn new(plan: Plan, catalog: &'a mut Catalog) -> Self {
    Self { plan, catalog }
  }

  // sad predicate pushdown :( doesn work as expected
  pub fn predicate_pushdown(&mut self, node: &mut Node) {
    let mut new_node: Option<Box<Node>> = None;

    match node {
      Node::Filter { source, condition } => {
        if let Node::Scan { filter, .. } = &mut **source {
          if filter.is_none() {
            *filter = Some(condition.clone());
            new_node = Some(source.clone());
          }
        }
      }
      Node::Limit { source, .. }
      | Node::Offset { source, .. }
      | Node::Projection { source, .. }
      | Node::GroupBy { source, .. }
      | Node::Having { source, .. }
      | Node::NestedLoopJoin { left: source, .. }
      | Node::HashJoin { left: source, .. }
      | Node::Sort { source, .. } => {
        new_node = Some(source.clone());
      }
      _ => {}
    }

    if let Some(mut new_node) = new_node {
      self.predicate_pushdown(&mut *new_node);
      *node = *new_node;
    }
  }

  pub fn optimize(&mut self) -> PhysicalPlan {
    let mut plan = self.plan.0.clone();
    self.predicate_pushdown(&mut plan);
    self.plan.0 = plan;
    self.create_physical_plan()
  }

  pub fn create_physical_plan(&self) -> PhysicalPlan {
    match &self.plan.0 {
      Node::CreateTable { schema } => {
        PhysicalPlan { node: Op::CreateTable { table: schema.clone() }, cost: None, childern: None }
      }
      Node::AlterTable { table, values } => {
        unimplemented!() // ovo ne radi ni planiranje majstore
      }
      Node::DropTable { table } => {
        PhysicalPlan { node: Op::DropTable { table_name: table.to_string() }, cost: None, childern: None }
      }
      Node::Insert { table, values } => {
        let data_source = table.to_string();
        let transformed_values = transform_values(values.clone());

        PhysicalPlan { node: Op::Insert { data_source, rows: transformed_values }, cost: None, childern: None }
      }
      _ => unimplemented!(),
    }
  }
}

fn transform_values(values: Vec<(Expression, Expression)>) -> Vec<HashMap<String, catalog::Value>> {
  let mut result = vec![];

  for (key_expr, value_expr) in values {
    let key = match key_expr {
      Expression::Identifier(identifier) => identifier,
      _ => panic!("Key must be an identifier"),
    };

    let value = match value_expr {
      Expression::Constant(constant) => convert_to_catalog_value(constant),
      _ => panic!("Value must be a constant"),
    };

    let mut map = HashMap::new();
    map.insert(key, value);
    result.push(map);
  }

  result
}

fn convert_to_catalog_value(value: Value) -> catalog::Value {
  match value {
    Value::Int(i) => catalog::Value::Int(i as i32),
    Value::Float(f) => catalog::Value::Float(f),
    Value::Text(s) => catalog::Value::Text(s),
    Value::Boolean(b) => catalog::Value::Boolean(b),
    _ => unimplemented!("Null values are not supported yet"),
  }
}
