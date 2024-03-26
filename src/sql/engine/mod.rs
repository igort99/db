use crate::storage::manager::BufferPool;
use super::optimizer::optimizer::{Op, PhysicalPlan};

#[derive(Debug)]
pub struct Executor<'a> {
    pub plan: PhysicalPlan,
    pub buffer_pool: &'a mut BufferPool
}

impl<'a> Executor<'a> {
    pub fn new(plan: PhysicalPlan, bp: &'a mut BufferPool) -> Self {
        Self { plan, buffer_pool: bp}
    }

    pub fn execute(&mut self) {
        match &self.plan.node {
            Op::CreateTable { table } => {
                self.buffer_pool.add_table_to_catalog(table.clone());
                // self.buffer_pool.
            }
            Op::DropTable { table_name } => {
                self.buffer_pool.remove_table_from_catalog(table_name)
            }
            Op::Insert { data_source, rows } => {

                unimplemented!()
            }
            _ => unimplemented!()
        }
    }
}