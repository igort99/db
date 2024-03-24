#![allow(unused)]
use crate::sql::{catalog::{self, catalog::Catalog}, planner::plan::{Node, Plan}};

pub struct PhysicalPlan {
  node: Op,
  cost: i32,
  childern: Vec<PhysicalPlan>
}

pub enum Op {
  TableScan { data_source: String, alias: Option<String> }, //filter
  IndexScan { data_source: String, alias: Option<String>, index: String }, // filter
  Projection { columns: Vec<String> },
}


// Has plan and catalog as attributes, so it can transform plan in phycal one
// and can consult catalog on the data structure
pub(crate) struct Optimizer<'a> {
  pub plan: Plan,
  pub catalog: &'a mut Catalog,
}
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

  pub fn optimize(&mut self) -> &Plan {
    let mut plan = self.plan.0.clone();
    self.predicate_pushdown(&mut plan);
    self.plan.0 = plan;
    &self.plan
  }

  pub fn create_physical_plan(&self) {
    unimplemented!()
  }

  // Eval if left and rigth side of binary exprs are same data types if not throw err

  pub fn create_table_physical_plan(&mut self) -> () {
    match &self.plan.0 {
      Node::CreateTable { schema } => {
        
      },
      _ => unimplemented!()
    }
  }
}
