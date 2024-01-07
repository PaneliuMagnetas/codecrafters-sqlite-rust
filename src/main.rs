mod record_handler;
mod sql_handler;
mod tokenizer;
mod tree_handler;

use anyhow::{anyhow, bail, Result};
use std::cell::RefCell;
use std::rc::Rc;
use std::vec::IntoIter;

use crate::record_handler::{RecordFormat, SchemaRecord};
use crate::sql_handler::{
    MappedSqlColumnVariant, MappedSqlStatement, MappedSqlWhereClause, SqlStatement,
};
use crate::tree_handler::{BTreeCell, BTreeLeafTableCell, BTreePage, PageReader};

static mut PAGE_READER: Option<PageReader> = None;

pub fn read_page(page_num: u32) -> Result<BTreePage> {
    if let Some(page_reader) = unsafe { &mut PAGE_READER } {
        page_reader.read_page(page_num)
    } else {
        panic!("page reader not initialized")
    }
}

fn init_global_page_reader(file_name: &str) -> Result<()> {
    unsafe {
        PAGE_READER = Some(PageReader::new(file_name)?);
    }

    Ok(())
}

pub struct CellIterator {
    cell_iter: IntoIter<BTreeCell>,
    right_most_pointer: Option<u32>,
    record_iter: Option<Box<CellIterator>>,
    get_index: usize,
    flag: bool,
}

impl CellIterator {
    pub fn new(page: BTreePage) -> Result<Self> {
        Ok(CellIterator {
            cell_iter: page.cells()?.into_iter(),
            right_most_pointer: page.right_most_pointer,
            record_iter: None,
            get_index: 0,
            flag: false,
        })
    }

    fn get(&mut self, index: usize) -> Option<BTreeLeafTableCell> {
        if let Some(records) = &mut self.record_iter {
            let record = records.get(index);

            if record.is_some() {
                return record;
            }
        }

        self.get_index = index;
        let value = self.next();
        self.get_index = 0;

        value
    }
}

impl Iterator for CellIterator {
    type Item = BTreeLeafTableCell;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.flag {
                return None;
            }

            if let Some(records) = &mut self.record_iter {
                let record = records.next();

                if record.is_some() {
                    return record;
                }
            }

            if let Some(cell) = self.cell_iter.next() {
                match cell {
                    BTreeCell::LeafTableCell(cell) => {
                        if self.get_index > 0 && cell.row_id.value < self.get_index as i64 {
                            continue;
                        }

                        if self.get_index > cell.row_id.value as usize {
                            return None;
                        }

                        return Some(cell);
                    }
                    BTreeCell::InteriorTableCell(cell) => {
                        if self.get_index > 0 && cell.row_id.value < self.get_index as i64 {
                            continue;
                        }

                        let b_tree_page = read_page(cell.left_child_page).unwrap();

                        self.record_iter = Some(Box::new(CellIterator {
                            cell_iter: b_tree_page.cells().unwrap().into_iter(),
                            right_most_pointer: b_tree_page.right_most_pointer,
                            record_iter: None,
                            get_index: self.get_index,
                            flag: false,
                        }));

                        self.get_index = 0;

                        continue;
                    }
                    _ => panic!("Records::next: cell is not table cell"),
                }
            } else {
                if let Some(right_most_pointer) = self.right_most_pointer {
                    let b_tree_page = read_page(right_most_pointer).unwrap();

                    self.record_iter = Some(Box::new(CellIterator {
                        cell_iter: b_tree_page.cells().unwrap().into_iter(),
                        right_most_pointer: b_tree_page.right_most_pointer,
                        record_iter: None,
                        get_index: self.get_index,
                        flag: false,
                    }));

                    self.right_most_pointer = None;
                    continue;
                }
            }

            self.flag = true;
        }
    }
}

struct IndexIterator {
    cell_iter: IntoIter<BTreeCell>,
    right_most_pointer: Option<u32>,
    index_iter: Option<Box<IndexIterator>>,
    record_iter: Rc<RefCell<CellIterator>>,
    keys: Rc<Vec<String>>,
    flag: bool,
}

impl IndexIterator {
    fn new(page: BTreePage, cell_iter: CellIterator, keys: Vec<String>) -> Result<Self> {
        Ok(IndexIterator {
            cell_iter: page.cells()?.into_iter(),
            index_iter: None,
            right_most_pointer: page.right_most_pointer,
            record_iter: Rc::new(RefCell::new(cell_iter)),
            keys: Rc::new(keys),
            flag: false,
        })
    }
}

impl Iterator for IndexIterator {
    type Item = BTreeLeafTableCell;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            if self.flag {
                return None;
            }

            if let Some(indices) = &mut self.index_iter {
                let index = indices.next();

                if index.is_some() {
                    return index;
                }
            }

            if let Some(cell) = self.cell_iter.next() {
                match cell {
                    BTreeCell::LeafIndexCell(cell) => {
                        let mut values = cell.values().unwrap();
                        let row_id = values.pop().unwrap().into();

                        for i in 0..self.keys.len() {
                            if String::from(&values[i]) != self.keys[i] {
                                continue 'outer;
                            }
                        }

                        let cell = self.record_iter.borrow_mut().get(row_id);

                        if cell.is_some() {
                            return cell;
                        }

                        continue;
                    }
                    BTreeCell::InteriorIndexCell(cell) => {
                        let b_tree_page = read_page(cell.left_child_page).unwrap();
                        let mut values = cell.values().unwrap();
                        values.pop().unwrap();

                        for i in 0..self.keys.len() {
                            if &String::from(&values[i]) < &self.keys[i] {
                                continue 'outer;
                            }
                        }

                        self.index_iter = Some(Box::new(IndexIterator {
                            cell_iter: b_tree_page.cells().unwrap().into_iter(),
                            right_most_pointer: b_tree_page.right_most_pointer,
                            keys: self.keys.clone(),
                            index_iter: None,
                            record_iter: self.record_iter.clone(),
                            flag: false,
                        }));

                        continue;
                    }
                    _ => panic!("Records::next: cell is not index cell"),
                }
            } else {
                if let Some(right_most_pointer) = self.right_most_pointer {
                    let b_tree_page = read_page(right_most_pointer).unwrap();

                    self.index_iter = Some(Box::new(IndexIterator {
                        cell_iter: b_tree_page.cells().unwrap().into_iter(),
                        right_most_pointer: b_tree_page.right_most_pointer,
                        keys: self.keys.clone(),
                        index_iter: None,
                        record_iter: self.record_iter.clone(),
                        flag: false,
                    }));

                    self.right_most_pointer = None;
                    continue;
                }
            }

            self.flag = true;
        }
    }
}

struct FilteredRecords {
    record_iter: Box<dyn Iterator<Item = BTreeLeafTableCell>>,
    mapped_sql_where_clause: MappedSqlWhereClause,
}

impl FilteredRecords {
    fn new(
        record_iter: Box<dyn Iterator<Item = BTreeLeafTableCell>>,
        mapped_sql_where_clause: MappedSqlWhereClause,
    ) -> Self {
        FilteredRecords {
            record_iter,
            mapped_sql_where_clause,
        }
    }
}

impl Iterator for FilteredRecords {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cell = self.record_iter.next()?;
            let record = Record::from(cell);

            if self.mapped_sql_where_clause.matches(&record) {
                return Some(record);
            }
        }
    }
}

pub struct Records {
    record_iter: Box<dyn Iterator<Item = Record>>,
    column_variants: Vec<MappedSqlColumnVariant>,
}

impl Records {
    fn new(
        cell_iter: Box<dyn Iterator<Item = BTreeLeafTableCell>>,
        sql_statement: MappedSqlStatement,
    ) -> Self {
        let mut record_iter: Box<dyn Iterator<Item = Record>> =
            if let Some(where_clause) = sql_statement.where_clause {
                Box::new(FilteredRecords::new(cell_iter, where_clause))
            } else {
                Box::new(cell_iter.map(|cell| Record::from(cell)))
            };

        match sql_statement.column_variants[0] {
            MappedSqlColumnVariant::Count => {
                let record_count = Record {
                    values: vec![RecordFormat::Integer64(record_iter.count() as i64)],
                };
                record_iter = Box::new(std::iter::once(record_count));
            }
            _ => {}
        }

        Records {
            record_iter,
            column_variants: sql_statement.column_variants,
        }
    }
}

impl Iterator for Records {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut values = Vec::new();
        let record_values = self.record_iter.next()?.values;

        for c in &self.column_variants {
            match c {
                MappedSqlColumnVariant::Column(index) => {
                    values.push(record_values[*index].clone());
                }
                MappedSqlColumnVariant::EveryColumn => {
                    values.extend(record_values.iter().map(|c| c.clone()));
                }
                MappedSqlColumnVariant::Count => {
                    values.push(record_values[0].clone());
                }
            }
        }

        Some(Record { values })
    }
}

#[derive(Debug)]
pub struct Record {
    values: Vec<RecordFormat>,
}

impl From<BTreeLeafTableCell> for Record {
    fn from(cell: BTreeLeafTableCell) -> Self {
        let mut values = cell.values().unwrap();

        if values[0] == RecordFormat::NULL {
            values[0] = RecordFormat::Integer64(cell.row_id.value);
        }

        Record { values }
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut values = self.values.iter();
        if let Some(value) = values.next() {
            write!(f, "{}", String::from(value))?;
        }

        for value in values {
            write!(f, "|{}", String::from(value))?;
        }

        Ok(())
    }
}

struct Schema {
    schema_records: Vec<SchemaRecord>,
}

impl<'a> Schema {
    fn new() -> Result<Self> {
        Ok(Schema {
            schema_records: read_page(1)?
                .iter()?
                .map(|r| SchemaRecord::from(r))
                .collect(),
        })
    }

    fn records(&self, sql_statement: SqlStatement) -> Result<Records> {
        let table = self
            .schema_records
            .iter()
            .find(|r| r.tbl_name == sql_statement.table_name)
            .ok_or_else(|| anyhow!("Table '{}' not found", sql_statement.table_name))?;

        let cell_iter = CellIterator::new(read_page(table.rootpage)?)?;

        if sql_statement.where_clause.is_some() {
            let index = self
                .schema_records
                .iter()
                .filter(|r| r.tbl_name == sql_statement.table_name && r.r#type == "index")
                .map(|r| {
                    let mapped_sql_statement = sql_statement.map(&table, Some(r));

                    let mut keys = Vec::new();

                    if mapped_sql_statement.is_ok() {
                        keys = mapped_sql_statement
                            .as_ref()
                            .unwrap()
                            .where_clause
                            .as_ref()
                            .unwrap()
                            .keys();
                    }

                    (r.rootpage, mapped_sql_statement, keys)
                })
                .max_by_key(|(_, _, keys)| keys.len())
                .unwrap();

            let (rootpage, mapped_sql_statement, keys) = index;

            if keys.len() > 0 {
                let index = read_page(rootpage)?;
                let index_iter = IndexIterator::new(index, cell_iter, keys)?;

                return Ok(Records::new(Box::new(index_iter), mapped_sql_statement?));
            }
        }

        let mapped_sql_statement = sql_statement.map(&table, None)?;
        Ok(Records::new(Box::new(cell_iter), mapped_sql_statement))
    }

    fn tables(&self) -> impl Iterator<Item = &SchemaRecord> {
        self.schema_records.iter().filter(|r| r.r#type == "table")
    }
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    init_global_page_reader(&args[1])?;

    let schema = Schema::new()?;

    match command.as_str() {
        ".dbinfo" => {
            let page_size = unsafe {
                PAGE_READER
                    .as_ref()
                    .expect("PAGE_READER should be initialized before any operation.")
                    .page_size()
            };
            println!("database page size: {}", page_size);
            println!("number of tables: {}", schema.tables().count());
        }
        ".tables" => {
            for table in schema.tables() {
                println!("{}", table.name);
            }
        }
        sql => {
            let sql_statement = SqlStatement::new(sql)?;
            for r in schema.records(sql_statement)? {
                println!("{}", r);
            }
        }
    }

    Ok(())
}
