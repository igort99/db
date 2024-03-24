use bincode::{deserialize_from, serialize_into};
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;

use crate::sql::catalog::catalog::{Catalog, Column, Table};
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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
  id: u32,            // page number
  page_checksum: u32, // page checksum, 0 if checksum disabled
  page_prev: u16,     // pointer to previous page
  page_next: u16,     // pointer to next page
  page_lower: u16,    // offset to start of free space
  page_upper: u16,    // offset to end of free space
  dirty: bool,        // dirty bit
  dbms_version: u32,  // dbms version
  origin_page: u32,   // origin page 1 or 0
}

impl PageHeader {
  pub fn new(
    id: u32,
    page_checksum: u32,
    page_prev: u16,
    page_next: u16,
    page_lower: u16,
    page_upper: u16,
    dirty: bool,
    dbms_version: u32,
    origin_page: u32,
  ) -> Self {
    Self { id, page_checksum, page_prev, page_next, page_lower, page_upper, dirty, dbms_version, origin_page }
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tuple {
  id: u32,     // tuple id = page_id + slot + offset
  length: u16, // length of tuple
  offset: u32, // offset to tuple
  data: Vec<u8>,
}

impl Tuple {
  pub fn new(id: u32, xact: u32, offset: u32, data: Vec<u8>) -> Self {
    let length = data.len() as u16;
    Self { id, length, offset, data }
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Slot {
  offset: u16, // offset to tuple
  length: u16, // length of tuple
  flags: u16,  // flags
}

impl Slot {
  pub fn new(offset: u16, length: u16, flags: u16) -> Self {
    Self { offset, length, flags }
  }

  pub fn read(&self) -> (u16, u16, u16) {
    (self.offset, self.length, self.flags)
  }

  pub fn write(&mut self, offset: u16, length: u16, flags: u16) {
    self.offset = offset;
    self.length = length;
    self.flags = flags;
  }

  pub fn is_free(&self) -> bool {
    self.length == 0
  }

  pub fn set_free(&mut self) {
    self.length = 0;
  }
}

impl Page {
  pub fn new(header: PageHeader, slots: Vec<Slot>, tuples: Vec<Tuple>) -> Self {
    Self { header, slots, tuples }
  }

  pub fn get_row(&self, index: usize) -> Option<&Tuple> {
    self.tuples.get(index)
  }

  pub fn add_tuple(&mut self, tuple: Tuple) -> Result<(), &'static str> {
    let tuple_length = tuple.length as u16;
    self.tuples.push(tuple);

    self.add_slot(self.header.page_upper, tuple_length);

    self.header.page_lower += 1;
    self.header.page_upper -= tuple_length as u16;

    Ok(())
  }

  pub fn insert_tuple(&mut self, data: Vec<u8>) -> Result<(), &'static str> {
    if self.is_full() {
      return Err("Page is full");
    }

    let offset = self.header.page_upper as u32;
    let tuple_id = self.header.id + self.tuples.len() as u32;
    let tuple = Tuple::new(tuple_id, 2, offset, data); // TODO: clean up the xact field if not used

    self.add_tuple(tuple);

    Ok(())
  }

  pub fn get_tuples_data(&self) -> Vec<Vec<u8>> {
    self.tuples.iter().map(|tuple| tuple.data.clone()).collect()
  }

  pub fn remove_tuple_by_id(&mut self, id: u32) -> Result<(), &'static str> {
    let index = self.tuples.iter().position(|tuple| tuple.id == id);

    if let Some(index) = index {
      let removed_tuple = self.tuples.remove(index);
      self.slots.remove(index);

      self.header.page_lower -= 1;
      self.header.page_upper += removed_tuple.length as u16;

      Ok(())
    } else {
      Err("Tuple not found")
    }
  }

  pub fn update_tuple_by_id(&mut self, id: u32, data: Vec<u8>) -> Result<(), &'static str> {
    let index = self.tuples.iter().position(|tuple| tuple.id == id);

    if let Some(index) = index {
      let tuple = &mut self.tuples[index];
      let old_length = tuple.data.len() as u16;
      let new_length = data.len() as u16;

      if self.header.page_upper - old_length < new_length {
        return Err("Not enough space in the page");
      }

      tuple.data = data;

      self.slots[index].length = new_length;

      self.header.page_upper = self.header.page_upper - new_length + old_length;

      Ok(())
    } else {
      Err("Tuple not found")
    }
  }

  pub fn add_slot(&mut self, offset: u16, length: u16) {
    let slot = Slot::new(offset, length, 0);

    self.slots.insert(0, slot);
  }

  pub fn is_full(&self) -> bool {
    self.header.page_lower as usize >= self.header.page_upper as usize
  }

  // //mock
  // pub fn insert_values(&mut self, values: &Vec<(Expression, Expression)>) {
  //   let mut data_as_u8 = Vec::new();

  //   for (data_value, _) in values {
  //     match data_value {
  //       Expression::Constant(value) => match value {
  //         plan::Value::Int(i) => data_as_u8.push(*i as u8),       // Convert the i32 to u8
  //         plan::Value::Text(s) => data_as_u8.push(s.len() as u8), // Convert the string length to u8
  //         _ => {}
  //       },
  //       _ => {}
  //     }
  //   }

  //   let tuple = Tuple::new(1, 2, 0, 0, 0, data_as_u8);
  //   self.tuples.push(tuple);
  // }

  // // uzeti podatkle catalog::catalog::Column i napraviti tuple od toga

  // pub fn add_tuple(&mut self, tuple: Tuple) {
  //   // must modify slots to and must check if there is enough space for tuple
  //   self.slots.push(Slot {
  //     offset: self.tuples.len() as u32,
  //     length: 0, // You might want to calculate the length based on the tuple
  //     flags: 0,  // You might want to set some flags here
  //   });

  //   self.tuples.push(tuple);
  // }

  // pub fn create_tuple_from_column(column: &Column) -> Result<Tuple, Box<bincode::ErrorKind>> {
  //   // Serialize the column into a byte array
  //   let data = bincode::serialize(column)?;

  //   // Create a new tuple with the serialized data
  //   // You'll need to decide how to set the flags, xact, and offset fields
  //   let flags = 0;
  //   let xact = 0;
  //   let offset = 0;

  //   Ok(Tuple { id: 0, length: 10, flags, xact, offset, data }) //mock
  // }

  // pub fn is_free(&self) -> bool {
  //   self.slots.is_empty()
  // }

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

// impl BufferPool {
//   pub fn new(num_pages: usize, catalog: Catalog) -> Self {
//     let mut pages = Vec::new();
//     for table in catalog.tables.values() {
//       let page = Page::new(table.clone());
//       pages.push(page);
//     }

//     pages.truncate(num_pages);
//     let dirty_pages = Vec::new();

//     Self { pages, catalog, dirty_pages }
//   }

//   pub fn read(&self, page_id: usize, slot_id: usize) -> Option<&Tuple> {
//     self.pages.get(page_id).and_then(|page| page.slots.get(slot_id).map(|slot| &page.tuples[slot.offset as usize]))
//   }

//   pub fn write(&mut self, page_id: usize, tuple: Tuple) -> bool {
//     if let Some(page) = self.pages.get_mut(page_id) {
//       page.slots.push(Slot {
//         offset: page.tuples.len() as u32,
//         length: 0, // You might want to calculate the length based on the tuple
//         flags: 0,  // You might want to set some flags here
//       });
//       page.tuples.push(tuple);
//       page.dirty = true;
//       self.dirty_pages.push(page_id);
//       true
//     } else {
//       false
//     }
//   }

//   pub fn find_dirty_page_by_origin(&mut self, origin: String) -> Option<&mut Page> {
//     let dirty_page_index = self.pages.iter().position(|page| page.header.origin == origin && page.dirty);
//     let free_page_index = self.pages.iter().position(|page| page.is_free());

//     if let Some(index) = dirty_page_index.or(free_page_index) {
//       Some(&mut self.pages[index])
//     } else {
//       None
//     }
//   }

//   pub fn get_catalog(&mut self) -> &mut Catalog {
//     &mut self.catalog
//   }

//   pub fn mark_page_dirty(&mut self, page_index: usize) {
//     self.pages[page_index].dirty = true;
//     self.dirty_pages.push(page_index);
//   }

//   pub fn persist_dirty_pages(&mut self) {
//     for &page_index in &self.dirty_pages {
//       if self.pages[page_index].dirty {
//         // write page to disk
//         // ...

//         self.pages[page_index].dirty = false;
//       }
//     }

//     self.dirty_pages.clear();
//   }
// }

// #[derive(Debug)] //mabybe
// pub struct DiscManager {
//   file: File,
// }

// id stanice fajl + offset + length
// page header treba da sadrzi koliko jos slotova moze da primi
// i koliko je slobodnog prostora
// verzija dbmsa treba da bude u page headeru

#[derive(Debug)]
pub struct StorageManager {
  page_size: usize,
}

impl StorageManager {
  pub fn new() -> Self {
    Self { page_size: PAGE_SIZE }
  }

  // pub fn read_page(&self, page_id: usize, slot_id: usize) -> io::Result<&Tuple> {
  //   self.buffer_pool.read(page_id, slot_id).ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to read page"))
  // }

  // pub fn write_page(&mut self, page_id: usize, tuple: Tuple) -> io::Result<()> {
  //   if self.buffer_pool.write(page_id, tuple) {
  //     Ok(())
  //   } else {
  //     Err(io::Error::new(io::ErrorKind::Other, "Failed to write page"))
  //   }
  // }

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

  // pub fn get_buffer_pool(&mut self) -> &mut BufferPool {
  //   &mut self.buffer_pool
  // }

  fn init_empty_catalog() -> Catalog {
    let catalog = Catalog { tables: HashMap::new() };
    let buffer = bincode::serialize(&catalog.tables).unwrap();
    let _ = Self::write_file(CATALOG_FILE, &buffer);

    catalog
  }

  pub fn write_catalog(&mut self, catalog: &Catalog) {
    let buffer = bincode::serialize(&catalog.tables).unwrap();
    let _ = Self::write_file(CATALOG_FILE, &buffer);
  }
}
//*Based on the discussion so far, here's a summary of what you need to do:
/*
1. **Add NULL Bitmap to Tuple**: Add a `Vec<bool>` to your `Tuple` struct to represent a bitmap for NULL values. Each boolean value will represent whether the corresponding field in the tuple is NULL (true means NULL, false means not NULL).

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tuple {
    // existing fields...
    null_bitmap: Vec<bool>, // bitmap for NULL values
}
```

2. **Update PageHeader**: Add fields to your `PageHeader` struct to keep track of the number of used slots and the offset of the starting location of the last used slot.

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PageHeader {
    // existing fields...
    num_used_slots: u32, // number of used slots
    last_used_slot_offset: usize, // offset of the starting location of last used slot
}
```

3. **Implement add_tuple Method**: Implement a method on the `Page` struct to add a tuple. This method should grow the slot array from the beginning to the end, and the data of the tuples should grow from the end to the beginning.

```rust
impl Page {
    pub fn add_tuple(&mut self, tuple: Tuple) {
        // implementation goes here
    }
}
```

4. **Implement is_full Method**: Implement a method on the `Page` struct to check if the page is full. The page is considered full when the slot array and the tuple data meet.

```rust
impl Page {
    pub fn is_full(&self) -> bool {
        // implementation goes here
    }
}
```

Please note that these are high-level steps and the actual implementation might vary based on your specific requirements and the exact way you're storing and managing data. */

/*
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
} */
