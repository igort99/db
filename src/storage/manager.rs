use bincode::{deserialize_from, serialize_into};
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;

use crate::sql::catalog::{Catalog, Column, Table};
use crate::sql::constants::{BUFFER_POOL_SIZE, CATALOG_FILE, PAGE_SIZE};
use crate::sql::planner::plan::{self, Expression};
use crate::sql::{self, catalog};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Page {
  header: PageHeader,
  tuples: Vec<Tuple>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
  id: u32,            // page number
  page_checksum: u32, // page checksum, 0 if checksum disabled
  page_prev: u16,     // pointer to previous page
  page_next: u16,     // pointer to next page
  dirty: i32,         // dirty bit
}

impl PageHeader {
  pub fn new(id: u32, page_checksum: u32, page_prev: u16, page_next: u16, dirty: i32) -> Self {
    Self { id, page_checksum, page_prev, page_next, dirty }
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tuple {
  id: u32,     // tuple id = page_id + offset
  length: u16, // length of tuple
  offset: u32, // offset to tuple
  data: Vec<u8>,
}

impl Tuple {
  pub fn new(id: u32, offset: u32, data: Vec<u8>) -> Self {
    Self { id, length: data.len() as u16, offset, data }
  }

  pub fn read(&self) -> &[u8] {
    &self.data
  }

  pub fn write(&mut self, data: Vec<u8>) {
    self.data = data;
  }

  pub fn is_free(&self) -> bool {
    self.length == 0
  }

  pub fn set_free(&mut self) {
    self.length = 0;
    self.data = Vec::new();
  }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct Slot {
//   offset: u16, // offset to tuple
//   length: u16, // length of tuple
//   flags: u16,  // flags
// }

// impl Slot {
//   pub fn new(offset: u16, length: u16, flags: u16) -> Self {
//     Self { offset, length, flags }
//   }

//   pub fn read(&self) -> (u16, u16, u16) {
//     (self.offset, self.length, self.flags)
//   }

//   pub fn write(&mut self, offset: u16, length: u16, flags: u16) {
//     self.offset = offset;
//     self.length = length;
//     self.flags = flags;
//   }

//   pub fn is_free(&self) -> bool {
//     self.length == 0
//   }

//   pub fn set_free(&mut self) {
//     self.length = 0;
//   }
// }

impl Page {
  pub fn new(header: PageHeader, tuples: Vec<Tuple>) -> Self {
    Self { header, tuples }
  }

  pub fn get_row(&self, index: usize) -> Option<&Tuple> {
    self.tuples.get(index)
  }

  pub fn add_tuple(&mut self, tuple: Tuple) -> Result<(), &'static str> {
    let tuple_length = tuple.length as u16;
    self.tuples.push(tuple);

    Ok(())
  }

  pub fn insert_tuple(&mut self, data: Vec<u8>) -> Result<(), &'static str> {
    // if self.is_full() {
    //   return Err("Page is full");
    // }

    // let offset = self.header.page_upper as u32;

    // let tuple_id = self.header.id + self.tuples.len() as u32;
    // let tuple = Tuple::new(tuple_id, 2, offset, data); // TODO: clean up the xact field if not used

    // self.add_tuple(tuple);

    Ok(())
  }

  pub fn get_tuples_data(&self) -> Vec<Vec<u8>> {
    self.tuples.iter().map(|tuple| tuple.data.clone()).collect()
  }

  pub fn remove_tuple_by_id(&mut self, id: u32) -> Result<(), &'static str> {
    let index = self.tuples.iter().position(|tuple| tuple.id == id);

    if let Some(index) = index {
      // let removed_tuple = self.tuples.remove(index);
      // self.slots.remove(index);

      // self.header.page_lower -= 1;
      // self.header.page_upper += removed_tuple.length as u16;

      Ok(())
    } else {
      Err("Tuple not found")
    }
  }

  // pub fn update_tuple_by_id(&mut self, id: u32, data: Vec<u8>) -> Result<(), &'static str> {
  //   let index = self.tuples.iter().position(|tuple| tuple.id == id);

  //   if let Some(index) = index {
  //     let tuple = &mut self.tuples[index];
  //     let old_length = tuple.data.len() as u16;
  //     let new_length = data.len() as u16;

  //     if self.header.page_upper - old_length < new_length {
  //       return Err("Not enough space in the page");
  //     }

  //     tuple.data = data;

  //     self.slots[index].length = new_length;

  //     self.header.page_upper = self.header.page_upper - new_length + old_length;

  //     Ok(())
  //   } else {
  //     Err("Tuple not found")
  //   }
  // }

  // pub fn add_slot(&mut self, offset: u16, length: u16) {
  //   let slot = Slot::new(offset, length, 0);

  //   self.slots.insert(0, slot);
  // }

  // pub fn is_full(&self) -> bool {
  //   self.header.page_lower as usize >= self.header.page_upper as usize
  // }
}

// Treba da radi perrsit na nekom, nivou , da prati prljave strancie, tj koje su menjane
// i da ih upisuje u fajl ako nisu samo da ih izbaci
#[derive(Debug)]
pub struct BufferPool {
  frames: Vec<Page>,
  catalog: Catalog,
  storage_manager: StorageManager,
  page_table: HashMap<u32, usize>,
  dirty_pages: HashSet<usize>,
  pin_count: Vec<u32>,
  origin_pages: HashMap<u32, u32>,
}

impl BufferPool {
  pub fn new() -> Self {
    let storage_manager = StorageManager::new();

    let frames = Vec::with_capacity(BUFFER_POOL_SIZE);
    let catalog = storage_manager.read_catalog().unwrap(); // TODO: Handle error
    let page_table = HashMap::new();
    let dirty_pages = HashSet::new();
    let pin_count = Vec::with_capacity(BUFFER_POOL_SIZE);
    let origin_pages = HashMap::new(); // should be loaded from disk but if there are no origin pages it should be empty

    Self { frames, catalog, storage_manager, page_table, dirty_pages, pin_count, origin_pages }
  }

  fn get_page(&mut self, page_id: u32) -> Option<&mut Page> {
    self.page_table.get(&page_id).and_then(|&index| self.frames.get_mut(index))
    // if not in buffer pool read from disk
  }

  fn remove_page(&mut self, page_id: u32) {
    if let Some(index) = self.page_table.remove(&page_id) {
      self.frames.remove(index);
      self.dirty_pages.remove(&index);
      self.pin_count.remove(index);
    }
  }

  pub fn add_table_to_catalog(&mut self, table: Table) {
    self.catalog.add_table(table);
    self.storage_manager.write_catalog(&self.catalog); // TODO: maybe decide when to write the catalog
  }

  pub fn get_catalog(&mut self) -> &mut Catalog {
    &mut self.catalog
  }

  pub fn remove_table_from_catalog(&mut self, table_name: &str) {
    self.catalog.remove_table(table_name);
    self.storage_manager.write_catalog(&self.catalog); // TODO: maybe decide when to write the catalog
  }

  // INFO: Do sequential read from disk and deseaialize pages and use data to return query result
  // INFO: maybe this is useless
  pub fn get_all_data_for_origin(&mut self, origin_page_id: u32) -> Result<Vec<u8>, &'static str> {
    let mut data = Vec::new();
    let mut current_page_id = origin_page_id;

    loop {
      let page = match self.get_page(current_page_id) {
        Some(page) => page,
        None => return Err("Page not found"),
      };

      data.extend(page.get_tuples_data().into_iter().flatten());

      if page.header.page_next == 0 {
        break;
      } else {
        current_page_id = page.header.page_next as u32;
      }
    }

    Ok(data)
  }
}

#[derive(Debug)]
pub struct StorageManager {
  page_size: usize,
}

impl StorageManager {
  pub fn new() -> Self {
    Self { page_size: PAGE_SIZE }
  }

  pub fn read_catalog(&self) -> io::Result<Catalog> {
    // TODO: must make it cleaner and refactor it
    let file_result = Self::read_file(CATALOG_FILE);

    let mut file = match file_result {
      Ok(file) => file,
      Err(e) => {
        if e.kind() == io::ErrorKind::NotFound {
          return Ok(Self::init_empty_catalog());
        } else {
          return Err(e);
        }
      }
    };

    let buffer = Self::file_to_buffer(&mut file)?;

    let tables = match bincode::deserialize(&buffer) {
      Ok(tables) => tables,
      Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
    };

    let catalog: Catalog = Catalog::new(tables);

    Ok(catalog)
    // idenx key kolokna i offset posto idemo sa fized size pages
  }

  fn read_file(file_name: &str) -> io::Result<File> {
    let file_path = Path::new(file_name);
    let file = File::open(file_path)?;

    Ok(file)
  }

  fn file_to_buffer(file: &mut File) -> io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    Ok(buffer)
  }

  fn write_file(file_name: &str, buffer: &[u8]) -> io::Result<()> {
    let file_path = Path::new(file_name);
    let mut file = File::create(file_path)?;

    file.write_all(buffer)?;

    Ok(())
  }

  fn init_empty_catalog() -> Catalog {
    let catalog = Catalog::new(HashMap::new());
    let buffer = bincode::serialize(&catalog.tables).unwrap();
    let _ = Self::write_file(CATALOG_FILE, &buffer);

    catalog
  }

  pub fn write_catalog(&mut self, catalog: &Catalog) {
    let buffer = bincode::serialize(&catalog.tables).unwrap();
    let _ = Self::write_file(CATALOG_FILE, &buffer);
  }

  pub fn create_file(&mut self, file_name: &String) {}
}
