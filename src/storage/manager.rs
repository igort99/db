use bincode::{deserialize_from, serialize_into};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;

use crate::sql::catalog::catalog::{Catalog, Column};
use crate::sql::constants::{BUFFER_POOL_SIZE, CATALOG_FILE, PAGE_SIZE};
use crate::sql::planner::plan::{self, Expression};
use crate::sql::{self, catalog};

// Create a new table file
// let table_name = "my_table";
// match create_table_file(table_name) {
//   Ok(_) => println!("Table file created successfully."),
//   Err(e) => println!("Failed to create table file: {}", e),
// }

// let mut columns = HashMap::new();
// columns.insert("id".to_string(), DataType::Integer);
// columns.insert("name".to_string(), DataType::String);
// let mut table = Table {
//   pages: Vec::new(),
//   header: TableHeader {
//     num_pages: 0,
//     page_size: 4096, // Set the page size to 4096 bytes
//     schema: Schema { columns: columns },
//   },
// };

// let data_values = vec![Data::Int(1), Data::String("Alice".to_string())];

// let mut data_as_u8 = Vec::new();

// for data_value in data_values {
//   match data_value {
//     Data::Int(i) => data_as_u8.push(i as u8),          // Convert the i32 to u8
//     Data::String(s) => data_as_u8.push(s.len() as u8), // Convert the string length to u8
//   }
// }

// // Create a tuple that matches the schema
// let tuple = Tuple {
//   id: 1,     // Set the id
//   length: 2, // Set the length
//   flags: 0,  // Set the flags
//   xact: 0,   // Set the xact
//   offset: 0, // Set the offset
//   data: data_as_u8,
// };

// // Create a new page
// let page = Page {
//   header: PageHeader {
//     id: 1,
//     page_checksum: 0,
//     page_prev: 0,
//     page_next: 0,
//     page_lower: 0,
//     page_upper: 0,
//     page_xact: 0,
//     page_items: 0,
//     page_free: 0,
//   },
//   slots: vec![],
//   tuples: vec![tuple],
// };

// let _ = table.add_page(page);

// // Write the page to the table file
// match write_table_to_file(&table, table_name) {
//   Ok(_) => println!("Page written to file successfully."),
//   Err(e) => println!("Failed to write page to file: {}", e),
// }
// }

// max page size is 4kb
// #[derive(Serialize, Deserialize, Debug)]
// struct Page {
//   header: PageHeader,
//   slots: Vec<Slot>,
//   tuples: Vec<Tuple>,
// }
// #[derive(Serialize, Deserialize, Debug)]
// struct Tuple {
//   // 4 bytes
//   id: u32, // tuple ID
//   // 2 bytes
//   length: u16, // length of tuple
//   // 2 bytes
//   flags: u16, // flags
//   // 4 bytes
//   xact: u32, // insert transaction ID stamp
//   // 4 bytes
//   offset: u32, // offset to tuple
//   // 4 bytes
//   data: Vec<u8>,
// }
// #[derive(Serialize, Deserialize, Debug)]
// struct Slot {
//   // 4 bytes
//   offset: u32, // offset to tuple
//   // 2 bytes
//   length: u16, // length of tuple
//   // 2 bytes
//   flags: u16, // flags
// }
// #[derive(Serialize, Deserialize, Debug)]
// struct PageHeader {
//   // 4 bytes
//   id: u32, // page number
//   // 4 bytes
//   page_checksum: u32, // page checksum, 0 if checksum disabled
//   // 2 bytes
//   page_prev: u16, // pointer to previous page
//   // 2 bytes
//   page_next: u16, // pointer to next page
//   // 2 bytes
//   page_lower: u16, // offset to start of free space
//   // 2 bytes
//   page_upper: u16, // offset to end of free space
//   // 4 bytes
//   page_xact: u32, // insert transaction ID stamp
//   // 4 bytes
//   page_items: u32, // number of items on this page
//   // 4 bytes
//   page_free: u32, // amount of free space on this page
// }

// #[derive(Serialize, Deserialize, Debug)]
// enum DataType {
//   Integer,
//   Float,
//   String,
//   Boolean,
//   Date,
//   DateTime,
//   Timestamp,
//   Null,
// }
// #[derive(Serialize, Deserialize, Debug)]
// enum Data {
//   Int(i32),
//   String(String),
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Schema {
//   columns: HashMap<String, DataType>,
// }

// fn create_table_file(table_name: &str) -> Result<(), Error> {
//   let filename = format!("{}.bin", table_name);
//   File::create(filename)?;
//   Ok(())
// }

// fn write_table_to_file(table: &Table, table_name: &str) -> Result<(), Error> {
//   let filename = format!("{}.bin", table_name);
//   let mut file = File::create(filename)?;

//   for page in &table.pages {
//     let data = bincode::serialize(page).unwrap();
//     file.write_all(&data)?;
//   }

//   Ok(())
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Table {
//   pages: Vec<Page>,
//   header: TableHeader,
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub struct TableHeader {
//   num_pages: u32,
//   page_size: u32,
//   schema: Schema,
//   // Add other fields as needed
// }

// impl Table {
//   pub fn add_page(&mut self, page: Page) -> std::io::Result<()> {
//     // Check if the last page is full
//     if let Some(last_page) = self.pages.last() {
//       let size =
//         bincode::serialized_size(last_page).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))? as usize;
//       if size >= self.header.page_size as usize {
//         // The last page is full, so add a new page
//         self.pages.push(page);
//         self.header.num_pages += 1;
//       } else {
//         // The last page is not full, so return an error
//         return Err(std::io::Error::new(std::io::ErrorKind::Other, "Last page is not full"));
//       }
//     } else {
//       // There are no pages yet, so add the new page
//       self.pages.push(page);
//       self.header.num_pages += 1;
//     }

//     Ok(())
//   }
// }

// fn read_tuples_from_page(page_buffer: &[u8]) -> io::Result<()> {
//     // Find the right slot in the page
//     // This depends on how your slots are organized in the page
//     // Here's a dummy example
//     let slot_offset = 0;  // Replace with your actual slot offset
//     let slot_length = 0;  // Replace with your actual slot length
//     let slot_data = &page_buffer[slot_offset..slot_offset + slot_length];

//     // Read the tuple from the slot
//     // This depends on how your tuples are organized in the slot
//     // Here's a dummy example
//     let tuple_offset = 0;  // Replace with your actual tuple offset
//     let tuple_length = 0;  // Replace with your actual tuple length
//     let tuple_data = &slot_data[tuple_offset..tuple_offset + tuple_length];

//     // Deserialize the tuple
//     // This depends on how your tuples are serialized
//     // Here's a dummy example
//     let tuple = deserialize_tuple(tuple_data)?;

//     Ok(())
// }

// // Function to open and read a file
// fn read_file(file_path: &str) -> io::Result<File> {
//     let file = File::open(file_path)?;
//     Ok(file)
// }

// fn deserialize_tuple(data: &[u8]) -> io::Result<()> {
//     // Replace with your actual deserialization logic
//     Ok(())
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Page {
  header: PageHeader,
  slots: Vec<Slot>,
  tuples: Vec<Tuple>,
  dirty: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
  schema: catalog::catalog::Table,
  origin: String,
  // 4 bytes
  id: u32, // page number
  // 4 bytes
  page_checksum: u32, // page checksum, 0 if checksum disabled
  // 2 bytes
  page_prev: u16, // pointer to previous page
  // 2 bytes
  page_next: u16, // pointer to next page
  // 2 bytes
  page_lower: u16, // offset to start of free space
  // 2 bytes
  page_upper: u16, // offset to end of free space
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tuple {
  // 4 bytes
  id: u32, // tuple ID
  // 2 bytes
  length: u16, // length of tuple
  // 2 bytes
  flags: u16, // flags
  // 4 bytes
  xact: u32, // insert transaction ID stamp
  // 4 bytes
  offset: u32, // offset to tuple
  // 4 bytes
  data: Vec<u8>,
}

impl Tuple {
  pub fn new(id: u32, length: u16, flags: u16, xact: u32, offset: u32, data: Vec<u8>) -> Self {
    Self { id, length, flags, xact, offset, data }
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Slot {
  // 4 bytes
  offset: u32, // offset to tuple
  // 2 bytes
  length: u16, // length of tuple
  // 2 bytes
  flags: u16, // flags
}

impl Page {
  pub fn new(table: catalog::catalog::Table) -> Self {
    Self {
      header: PageHeader {
        schema: table,
        origin: "disk".to_string(), //test
        id: 0,
        page_checksum: 0,
        page_prev: 0,
        page_next: 0,
        page_lower: 0,
        page_upper: 0,
      },
      slots: Vec::new(),
      tuples: Vec::new(),
      dirty: false,
    }
  }

  //mock
  pub fn insert_values(&mut self, values: &Vec<(Expression, Expression)>) {
    let mut data_as_u8 = Vec::new();

    for (data_value, _) in values {
      match data_value {
        Expression::Constant(value) => match value {
          plan::Value::Int(i) => data_as_u8.push(*i as u8),       // Convert the i32 to u8
          plan::Value::Text(s) => data_as_u8.push(s.len() as u8), // Convert the string length to u8
          _ => {}
        },
        _ => {}
      }
    }

    let tuple = Tuple::new(1, 2, 0, 0, 0, data_as_u8);
    self.tuples.push(tuple);
  }

  // uzeti podatkle catalog::catalog::Column i napraviti tuple od toga

  pub fn add_tuple(&mut self, tuple: Tuple) {
    // must modify slots to and must check if there is enough space for tuple
    self.slots.push(Slot {
      offset: self.tuples.len() as u32,
      length: 0, // You might want to calculate the length based on the tuple
      flags: 0,  // You might want to set some flags here
    });

    self.tuples.push(tuple);
  }

  pub fn create_tuple_from_column(column: &Column) -> Result<Tuple, Box<bincode::ErrorKind>> {
    // Serialize the column into a byte array
    let data = bincode::serialize(column)?;

    // Create a new tuple with the serialized data
    // You'll need to decide how to set the flags, xact, and offset fields
    let flags = 0;
    let xact = 0;
    let offset = 0;

    Ok(Tuple { id: 0, length: 10, flags, xact, offset, data }) //mock
  }

  pub fn is_free(&self) -> bool {
    self.slots.is_empty()
  }

  // pub fn read(&self, offset: usize, length: usize) -> Option<&[u8]> {
  //   self.data.get(offset..offset + length)
  // }

  // pub fn write(&mut self, offset: usize, data: &[u8]) -> bool {
  //   if offset + data.len() <= self.data.len() {
  //     self.data[offset..offset + data.len()].copy_from_slice(data);
  //     true
  //   } else {
  //     false
  //   }
  // }
}

// Treba da radi perrsit na nekom, nivou , da prati prljave strancie, tj koje su menjane
// i da ih upisuje u fajl ako nisu samo da ih izbaci
#[derive(Debug)]
pub struct BufferPool {
  pages: Vec<Page>,
  catalog: Catalog,
  dirty_pages: Vec<usize>, // indices of dirty pages
}

impl BufferPool {
  pub fn new(num_pages: usize, catalog: Catalog) -> Self {
    let mut pages = Vec::new();
    for table in catalog.tables.values() {
      let page = Page::new(table.clone());
      pages.push(page);
    }

    pages.truncate(num_pages);
    let dirty_pages = Vec::new();

    Self { pages, catalog, dirty_pages }
  }

  pub fn read(&self, page_id: usize, slot_id: usize) -> Option<&Tuple> {
    self.pages.get(page_id).and_then(|page| page.slots.get(slot_id).map(|slot| &page.tuples[slot.offset as usize]))
  }

  pub fn write(&mut self, page_id: usize, tuple: Tuple) -> bool {
    if let Some(page) = self.pages.get_mut(page_id) {
      page.slots.push(Slot {
        offset: page.tuples.len() as u32,
        length: 0, // You might want to calculate the length based on the tuple
        flags: 0,  // You might want to set some flags here
      });
      page.tuples.push(tuple);
      page.dirty = true;
      self.dirty_pages.push(page_id);
      true
    } else {
      false
    }
  }

  pub fn find_dirty_page_by_origin(&mut self, origin: String) -> Option<&mut Page> {
    let dirty_page_index = self.pages.iter().position(|page| page.header.origin == origin && page.dirty);
    let free_page_index = self.pages.iter().position(|page| page.is_free());

    if let Some(index) = dirty_page_index.or(free_page_index) {
      Some(&mut self.pages[index])
    } else {
      None
    }
  }

  pub fn get_catalog(&mut self) -> &mut Catalog {
    &mut self.catalog
  }

  pub fn mark_page_dirty(&mut self, page_index: usize) {
    self.pages[page_index].dirty = true;
    self.dirty_pages.push(page_index);
  }

  pub fn persist_dirty_pages(&mut self) {
    for &page_index in &self.dirty_pages {
      if self.pages[page_index].dirty {
        // write page to disk
        // ...

        self.pages[page_index].dirty = false;
      }
    }

    self.dirty_pages.clear();
  }
}

// #[derive(Debug)] //mabybe
// pub struct DiscManager {
//   file: File,
// }

#[derive(Debug)]
pub struct StorageManager {
  buffer_pool: BufferPool,
  page_size: usize,
  // disc_manager: DiscManager,
}

impl StorageManager {
  pub fn new() -> Self {
    let catalog = Self::read_catalog().unwrap();
    let buffer_pool = BufferPool::new(BUFFER_POOL_SIZE, catalog);

    Self { buffer_pool, page_size: PAGE_SIZE }
  }

  pub fn read_page(&self, page_id: usize, slot_id: usize) -> io::Result<&Tuple> {
    self.buffer_pool.read(page_id, slot_id).ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to read page"))
  }

  pub fn write_page(&mut self, page_id: usize, tuple: Tuple) -> io::Result<()> {
    if self.buffer_pool.write(page_id, tuple) {
      Ok(())
    } else {
      Err(io::Error::new(io::ErrorKind::Other, "Failed to write page"))
    }
  }

  pub fn read_catalog() -> io::Result<Catalog> {
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

  pub fn get_buffer_pool(&mut self) -> &mut BufferPool {
    &mut self.buffer_pool
  }

  fn init_empty_catalog() -> Catalog {
    let catalog = Catalog { tables: HashMap::new() };
    let buffer = bincode::serialize(&catalog.tables).unwrap();
    let _ = Self::write_file(CATALOG_FILE, &buffer);

    catalog
  }

  pub fn write_catalog(&mut self) {
    let catalog = self.buffer_pool.get_catalog();
    let buffer = bincode::serialize(&catalog.tables).unwrap();
    let _ = Self::write_file(CATALOG_FILE, &buffer);
  }

  pub fn get_catalog(&self) -> &Catalog {
    &self.buffer_pool.catalog
  }
}
