#![allow(unused)]
use crate::sql::planner::plan::{Node, Plan};

pub(crate) struct Optimizer {
  pub plan: Plan,
}
// treba da vrati fizicki plan ali da prte toga uradi optimizaciju
// predicate push down i projection push down
// treba da se implementira i cost based optimization
// treba da se implementira i join ordering

impl Optimizer {
  pub fn new(plan: Plan) -> Self {
    Self { plan }
  }

  // sad predicate pushdown :(
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
}
