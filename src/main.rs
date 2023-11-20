mod tokenizer;

use anyhow::{anyhow, bail, Result};
use std::fmt::Write;
use std::fmt::{self, Formatter};
use std::fs::File;
use std::io::{Read, Seek};
use std::rc::Rc;
use std::slice::Iter;
use std::vec::IntoIter;

use crate::tokenizer::{Token, Tokenizer};

#[derive(Debug)]
enum BTreeCell {
    // InteriorIndexCell(BTreeInteriorIndexCell),
    InteriorTableCell(BTreeInteriorTableCell),
    // LeafIndexCell(BTreeLeafIndexCell),
    LeafTableCell(BTreeLeafTableCell),
}

#[derive(Debug)]
struct BTreeInteriorTableCell {
    left_child_page: u32,
    row_id: Varint,
}

#[allow(dead_code)]
#[derive(Debug)]
struct BTreeLeafTableCell {
    payload_size: Varint,
    row_id: Varint,
    values: Vec<RecordFormat>,
    first_overflow_page: u32,
}

impl BTreeLeafTableCell {}

#[derive(Debug)]
struct Varint {
    value: i64,
    size: u8,
}

#[derive(Debug, PartialEq)]
enum RecordFormat {
    NULL,
    Integer8(i8),
    Integer16(i16),
    Integer24(i32),
    Integer32(i32),
    Integer48(i64),
    Integer64(i64),
    Float64(f64),
    Integer0,
    Integer1,
    Blob(Vec<u8>),
    String(String),
}

impl RecordFormat {
    fn new(payload: &[u8], value: i64) -> Result<(Self, &[u8])> {
        match value {
            0 => Ok((RecordFormat::NULL, payload)),
            1 => {
                let buf: [u8; 1] = [payload[0]];
                Ok((
                    RecordFormat::Integer8(i8::from_be_bytes(buf)),
                    &payload[1..],
                ))
            }
            2 => {
                let buf: [u8; 2] = [payload[0], payload[1]];
                Ok((
                    RecordFormat::Integer16(i16::from_be_bytes(buf)),
                    &payload[2..],
                ))
            }
            3 => {
                let buf: [u8; 4] = [0, payload[0], payload[1], payload[2]];
                Ok((
                    RecordFormat::Integer24(i32::from_be_bytes(buf)),
                    &payload[3..],
                ))
            }
            4 => {
                let buf: [u8; 4] = [payload[0], payload[1], payload[2], payload[3]];
                Ok((
                    RecordFormat::Integer32(i32::from_be_bytes(buf)),
                    &payload[4..],
                ))
            }
            5 => {
                let buf: [u8; 8] = [
                    0, 0, payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                ];
                Ok((
                    RecordFormat::Integer48(i64::from_be_bytes(buf)),
                    &payload[6..],
                ))
            }
            6 => {
                let buf: [u8; 8] = [
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ];
                Ok((
                    RecordFormat::Integer64(i64::from_be_bytes(buf)),
                    &payload[8..],
                ))
            }
            7 => {
                let buf: [u8; 8] = [
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ];
                Ok((
                    RecordFormat::Float64(f64::from_be_bytes(buf)),
                    &payload[8..],
                ))
            }
            8 => Ok((RecordFormat::Integer0, payload)),
            9 => Ok((RecordFormat::Integer1, payload)),
            10 | 11 => bail!("Invalid record format"),
            _ => {
                if value % 2 == 0 {
                    let size = (value - 12) / 2;
                    let buf = &payload[..size as usize];
                    Ok((RecordFormat::Blob(buf.to_vec()), &payload[size as usize..]))
                } else {
                    let size = (value - 13) / 2;
                    let buf = &payload[..size as usize];
                    let string = String::from_utf8_lossy(buf).to_string();
                    Ok((RecordFormat::String(string), &payload[size as usize..]))
                }
            }
        }
    }
}

impl PartialEq<Token<'_>> for RecordFormat {
    fn eq(&self, other: &Token) -> bool {
        match other {
            Token::Number(n) => match self {
                RecordFormat::Integer8(v) => *n == *v as i64,
                RecordFormat::Integer16(v) => *n == *v as i64,
                RecordFormat::Integer24(v) => *n == *v as i64,
                RecordFormat::Integer32(v) => *n == *v as i64,
                RecordFormat::Integer48(v) => *n == *v,
                RecordFormat::Integer64(v) => *n == *v,
                _ => false,
            },
            Token::Text(t) => match self {
                RecordFormat::NULL => *t == "NULL",
                _ => false,
            },
            Token::String(s) => match self {
                RecordFormat::String(v) => s == v,
                _ => false,
            },
            _ => false,
        }
    }

    fn ne(&self, other: &Token) -> bool {
        !self.eq(other)
    }
}

impl fmt::Display for RecordFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            RecordFormat::NULL => write!(f, "NULL"),
            RecordFormat::Integer8(v) => write!(f, "{}", v),
            RecordFormat::Integer16(v) => write!(f, "{}", v),
            RecordFormat::Integer24(v) => write!(f, "{}", v),
            RecordFormat::Integer32(v) => write!(f, "{}", v),
            RecordFormat::Integer48(v) => write!(f, "{}", v),
            RecordFormat::Integer64(v) => write!(f, "{}", v),
            RecordFormat::Float64(v) => write!(f, "{}", v),
            RecordFormat::Integer0 => write!(f, "0"),
            RecordFormat::Integer1 => write!(f, "1"),
            RecordFormat::Blob(v) => write!(f, "{:?}", v),
            RecordFormat::String(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug)]
struct Record {
    row_id: i64,
    columns: Vec<Rec>,
}

#[derive(Debug)]
struct Rec {
    name: String,
    value: RecordFormat,
}

struct ValueIter<'a> {
    record: &'a Record,
    column_iter: Iter<'a, Rec>,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = &'a RecordFormat;

    fn next(&mut self) -> Option<Self::Item> {
        Some(&self.column_iter.next()?.value)
    }
}

impl<'a> Record {
    fn new(b_tree_cell: BTreeCell, column_names: Option<Rc<Vec<String>>>) -> Result<Self> {
        let b_tree_leaf_table_cell = match b_tree_cell {
            BTreeCell::LeafTableCell(cell) => cell,
            _ => bail!("Invalid BTreeCell"),
        };

        let mut columns = Vec::new();

        let mut values = b_tree_leaf_table_cell.values.into_iter();
        let len = values.len();
        for i in 0..len {
            let name = if let Some(column_names) = &column_names {
                column_names[i].clone()
            } else {
                "".to_string()
            };

            let mut value = values.next().unwrap();

            if i == 0 && value == RecordFormat::NULL {
                value = RecordFormat::Integer64(b_tree_leaf_table_cell.row_id.value);
            }

            columns.push(Rec { name, value });
        }

        Ok(Record {
            row_id: b_tree_leaf_table_cell.row_id.value,
            columns,
        })
    }

    fn values(&'a self) -> ValueIter<'a> {
        ValueIter {
            record: self,
            column_iter: self.columns.iter(),
        }
    }
}

impl Varint {
    fn from(buf: &[u8]) -> (Self, &[u8]) {
        if buf.is_empty() {
            panic!("Varint::from: buf is empty");
        }

        let mut result = 0;
        let mut size = 0;

        for byte in buf {
            result = (result << 7) | (byte & 0x7f) as i64;
            size += 1;

            if byte & 0x80 == 0 {
                break;
            }
        }

        (
            Varint {
                value: result,
                size: size as u8,
            },
            &buf[size..],
        )
    }
}

#[derive(PartialEq, Debug)]
enum BTreePageType {
    // InteriorIndexPage = 0x02,
    InteriorTablePage = 0x05,
    // LeafIndexPage = 0x0a,
    LeafTablePage = 0x0d,
}

#[allow(dead_code)]
#[derive(Debug)]
struct BTreePage {
    page_type: BTreePageType,
    first_freeblock_offset: u16,
    num_cells: u16,
    cell_content_area: u16,
    fragment_bytes: u8,
    right_most_pointer: u32,
    cell_pointers: Vec<u16>,
    cells: Vec<BTreeCell>,
}

struct Records<'a> {
    cell_iter: Option<IntoIter<BTreeCell>>,
    record_iter: Option<IntoIter<BTreeCell>>,
    sql_where_clause: Option<&'a SqlWhereClause<'a>>,
    flag: bool,
    page_reader: &'a PageReader,
    column_names: Option<Rc<Vec<String>>>,
}

impl<'a> Records<'a> {
    fn new(page: BTreePage, page_reader: &'a PageReader) -> Result<Self> {
        let mut cell_iter = None;
        let mut record_iter = None;

        match page.page_type {
            BTreePageType::LeafTablePage => {
                record_iter = Some(page.cells.into_iter());
            }
            BTreePageType::InteriorTablePage => {
                cell_iter = Some(page.cells.into_iter());
            }
        }

        Ok(Records {
            cell_iter,
            record_iter,
            sql_where_clause: None,
            flag: false,
            page_reader,
            column_names: None,
        })
    }

    fn named(self, schema_record: Rc<SchemaRecord>) -> Result<Self> {
        Ok(Records {
            cell_iter: self.cell_iter,
            record_iter: self.record_iter,
            sql_where_clause: self.sql_where_clause,
            flag: self.flag,
            page_reader: self.page_reader,
            column_names: Some(Rc::new(schema_record.column_names()?)),
        })
    }

    fn r#where(self, sql_where_clause: &'a SqlWhereClause<'a>) -> Self {
        Records {
            cell_iter: self.cell_iter,
            record_iter: self.record_iter,
            flag: self.flag,
            sql_where_clause: Some(sql_where_clause),
            page_reader: self.page_reader,
            column_names: self.column_names,
        }
    }
}

impl<'a> Iterator for Records<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.flag {
                return None;
            }

            if let Some(records) = &mut self.record_iter {
                if let Some(record) = records.next() {
                    let record = match &self.column_names {
                        Some(column_names) => {
                            Record::new(record, Some(column_names.clone())).unwrap()
                        }
                        None => Record::new(record, None).unwrap(),
                    };

                    if let Some(sql_where_clause) = &self.sql_where_clause {
                        if !sql_where_clause.matches(&record) {
                            continue;
                        }
                    }

                    return Some(record);
                }
            }

            if let Some(cells) = &mut self.cell_iter {
                if let Some(cell) = cells.next() {
                    match cell {
                        BTreeCell::InteriorTableCell(cell) => {
                            let page = self.page_reader.read_page(cell.left_child_page).unwrap();
                            let b_tree_page = BTreePage::new(&page).unwrap();
                            self.record_iter = Some(b_tree_page.cells.into_iter());
                            continue;
                        }
                        _ => panic!("Invalid BTreeCell"),
                    }
                }
            }

            self.flag = true;
        }
    }
}

impl<'a> BTreePage {
    fn new(page: &[u8]) -> Result<Self> {
        let mut b_tree_page = Self::read_header(&page)?;
        b_tree_page.read_cells(page)?;

        Ok(b_tree_page)
    }

    fn with_offset_header(page: &[u8], offset: usize) -> Result<Self> {
        let mut b_tree_page = Self::read_header(&page[offset..])?;
        b_tree_page.read_cells(page)?;

        Ok(b_tree_page)
    }

    fn read_cells(&mut self, page: &[u8]) -> Result<()> {
        let mut cells = Vec::new();
        for cell_pointer in &self.cell_pointers {
            let page_slice = &page[*cell_pointer as usize..];
            cells.push(match self.page_type {
                BTreePageType::InteriorTablePage => {
                    let left_child_page = u32::from_be_bytes(page_slice[..4].try_into()?);
                    let (row_id, _) = Varint::from(&page_slice[4..]);

                    BTreeCell::InteriorTableCell(BTreeInteriorTableCell {
                        left_child_page,
                        row_id,
                    })
                }
                BTreePageType::LeafTablePage => {
                    let (payload_size, page_slice) = Varint::from(page_slice);
                    let (row_id, page_slice) = Varint::from(page_slice);

                    let payload_size_val = payload_size.value as usize;
                    BTreeCell::LeafTableCell(BTreeLeafTableCell {
                        payload_size,
                        row_id,
                        values: self.read_values(&page_slice[..(payload_size_val as usize)])?,
                        first_overflow_page: 0,
                    })
                }
            });
        }

        self.cells = cells;

        Ok(())
    }

    fn read_values(&self, payload: &[u8]) -> Result<Vec<RecordFormat>> {
        let mut header = Vec::new();
        let mut body = Vec::new();
        let mut payload = payload;

        let (header_size_varint, remaining) = Varint::from(payload);
        payload = remaining;
        let mut header_size = header_size_varint.value - header_size_varint.size as i64;

        while header_size > 0 {
            let (varint, remaining) = Varint::from(payload);
            payload = remaining;
            header_size -= varint.size as i64;
            header.push(varint);
        }

        for v in &header {
            let (record_format, remaining) = RecordFormat::new(payload, v.value)?;
            payload = remaining;
            body.push(record_format);
        }

        Ok(body)
    }

    fn read_header(page: &[u8]) -> Result<Self> {
        let page_type = match page[0] {
            // 0x02 => BTreePageType::InteriorIndexPage,
            0x05 => BTreePageType::InteriorTablePage,
            // 0x0a => BTreePageType::LeafIndexPage,
            0x0d => BTreePageType::LeafTablePage,
            _ => bail!("Invalid page type {}", page[0]),
        };

        let mut header_len = 8;
        let first_freeblock_offset = u16::from_be_bytes([page[1], page[2]]);
        let num_cells = u16::from_be_bytes([page[3], page[4]]);
        let cell_content_area = u16::from_be_bytes([page[5], page[6]]);
        let fragment_bytes = page[7];
        let mut right_most_pointer = 0;
        // if page_type == BTreePageType::InteriorIndexPage
        //     ||
        if page_type == BTreePageType::InteriorTablePage {
            right_most_pointer = u32::from_be_bytes([page[8], page[9], page[10], page[11]]);
            header_len += 4;
        }

        let mut cell_pointers = Vec::new();

        let cell_pointer_size = num_cells as usize * 2 + header_len;

        for chunk in page[header_len..cell_pointer_size].chunks(2) {
            cell_pointers.push(u16::from_be_bytes([chunk[0], chunk[1]]));
        }

        Ok(BTreePage {
            page_type,
            first_freeblock_offset,
            num_cells,
            cell_content_area,
            fragment_bytes,
            right_most_pointer,
            cell_pointers,
            cells: vec![],
        })
    }
}

#[derive(Debug)]
struct SchemaTable {
    records: Vec<Rc<SchemaRecord>>,
}

impl SchemaTable {
    fn new(records: Records) -> Self {
        Self {
            records: records
                .map(|r| Rc::new(SchemaRecord::from(r)))
                .collect::<Vec<_>>(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct SchemaRecord {
    r#type: String,
    name: String,
    tbl_name: String,
    rootpage: u32,
    sql: String,
}

impl From<Record> for SchemaRecord {
    fn from(record: Record) -> Self {
        let mut column_iter = record.values();

        let r#type = match column_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let name = match column_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let tbl_name = match column_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let rootpage = match column_iter.next() {
            Some(RecordFormat::Integer8(i)) => *i as i64,
            Some(RecordFormat::Integer16(i)) => *i as i64,
            Some(RecordFormat::Integer24(i)) => *i as i64,
            Some(RecordFormat::Integer48(i)) => *i as i64,
            Some(RecordFormat::Integer64(i)) => *i as i64,
            _ => panic!("Invalid record format"),
        };

        let sql = match column_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        SchemaRecord {
            r#type: r#type.to_string(),
            name: name.to_string(),
            tbl_name: tbl_name.to_string(),
            rootpage: rootpage as u32,
            sql: sql.to_string(),
        }
    }
}

impl SchemaRecord {
    fn column_names(&self) -> Result<Vec<String>> {
        let mut tokenizer = Tokenizer::new(&self.sql);

        tokenizer.tag("CREATE")?;
        tokenizer.tag("TABLE")?;

        match tokenizer.next() {
            Some(Token::Text(s)) => {
                if s != self.name {
                    bail!("Invalid SQL create statement");
                }
            }
            Some(Token::String(s)) => {
                if s != self.name {
                    bail!("Invalid SQL create statement");
                }
            }
            _ => bail!("Invalid SQL create statement"),
        }

        tokenizer.tag("(")?;

        let mut result = Vec::new();

        loop {
            let tokens = tokenizer
                .take_while(|t| *t != Token::Punctuation(',') && *t != Token::Punctuation(')'));

            let token = match tokens[0] {
                Token::Text(s) => s.to_string(),
                _ => bail!("Invalid SQL create statement"),
            };

            result.push(token);

            match tokenizer.next() {
                Some(Token::Punctuation(')')) => break Ok(result),
                _ => (),
            }
        }
    }
}

#[derive(Debug)]
struct SqlStatement<'a> {
    column_variants: Vec<SqlColumnVariant<'a>>,
    table_name: &'a str,
    where_clause: Option<SqlWhereClause<'a>>,
}

impl<'a> SqlStatement<'a> {
    fn new(sql: &'a str) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(sql);
        let mut column_variants = Vec::new();

        tokenizer.tag("select")?;

        loop {
            let mut result = Err(anyhow!("Invalid SQL statement"));
            match tokenizer.next() {
                Some(token) => match token {
                    Token::Text(t) => {
                        match t.to_lowercase().as_str() {
                            "from" => break,
                            "count" => {
                                tokenizer.tag("(")?;
                                if let Some(_) = tokenizer.next() {
                                    result = Ok(SqlColumnVariant::Count);
                                }
                                tokenizer.tag(")")?;
                            }
                            _ => (),
                        }

                        if result.is_err() {
                            result = Ok(SqlColumnVariant::Column(t));
                        }
                    }
                    Token::Punctuation(',') => continue,
                    Token::Punctuation('*') => result = Ok(SqlColumnVariant::EveryColumn),
                    _ => bail!("Invalid SQL statement"),
                },
                None => bail!("Invalid SQL statement"),
            };

            column_variants.push(result?);
        }

        let table = match tokenizer.next() {
            Some(Token::Text(table)) => table,
            _ => bail!("Invalid SQL statement"),
        };

        let mut where_clause = None;

        if let Some(_) = tokenizer.peek() {
            where_clause = Some(SqlWhereClause::new(tokenizer.remaining())?);
        }

        Ok(SqlStatement {
            column_variants,
            table_name: table,
            where_clause,
        })
    }

    fn validate(&self, schema_record: Rc<SchemaRecord>) -> Result<()> {
        let column_names = schema_record.column_names()?;
        let columns = self
            .column_variants
            .iter()
            .filter(|c| match c {
                SqlColumnVariant::Column(column) => true,
                _ => false,
            })
            .map(|c| match c {
                SqlColumnVariant::Column(column) => column,
                _ => unreachable!(),
            });

        for c in columns {
            if !column_names.contains(&c.to_string()) {
                bail!("Invalid column name");
            }
        }

        if let Some(where_clause) = &self.where_clause {
            where_clause.validate(&schema_record)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct SqlWhereClause<'a> {
    columns: Vec<SqlWhereColumn<'a>>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct SqlWhereColumn<'a> {
    column: &'a str,
    operator: SqlOperator,
    value: Token<'a>,
}

impl SqlWhereColumn<'_> {
    fn matches(&self, record: &Record) -> bool {
        let val = record.columns.iter().find(|c| c.name == self.column);

        match self.operator {
            SqlOperator::Equal => return val.unwrap().value == self.value,
        }
    }
}

impl<'a> SqlWhereClause<'a> {
    fn new(sql: &'a str) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(sql);
        let mut columns = Vec::new();

        tokenizer.tag("where")?;

        loop {
            if tokenizer.peek().is_none() {
                break;
            }

            let column = match tokenizer.next() {
                Some(Token::Text(column)) => column,
                _ => bail!("Invalid where clause"),
            };

            let operator = match tokenizer.next() {
                Some(Token::Punctuation('=')) => SqlOperator::Equal,
                _ => bail!("Invalid operator in where clause"),
            };

            let value = tokenizer
                .next()
                .ok_or_else(|| anyhow!("Missing value in where clause"))?;

            if let Some(_) = tokenizer.peek() {
                bail!("Invalid where clause");
            }

            columns.push(SqlWhereColumn {
                column,
                operator,
                value,
            });
        }

        Ok(SqlWhereClause { columns })
    }

    fn validate(&self, schema_record: &SchemaRecord) -> Result<()> {
        for column in &self.columns {
            if !schema_record
                .column_names()?
                .contains(&column.column.to_string())
            {
                bail!("Invalid column name {} in where clause", column.column);
            }
        }

        Ok(())
    }

    fn matches(&self, record: &Record) -> bool {
        for column in &self.columns {
            if !column.matches(record) {
                return false;
            }
        }

        true
    }
}

#[derive(Debug)]
enum SqlOperator {
    Equal,
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    let schema = Schema::new(&args[1])?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", schema.page_reader.page_size);
            println!("number of tables: {}", schema.tables().count());
        }
        ".tables" => {
            for table in schema.tables() {
                println!("{}", table.name);
            }
        }
        sql => {
            let sql_statement = SqlStatement::new(sql)?;
            let records = schema.records(&sql_statement)?;

            if sql_statement.column_variants.len() == 1
                && sql_statement.column_variants[0] == SqlColumnVariant::Count
            {
                println!("{}", records.count());
                return Ok(());
            }

            for record in records {
                let mut result = String::new();
                for c in &sql_statement.column_variants {
                    match c {
                        SqlColumnVariant::Column(column) => {
                            let column = record.columns.iter().find(|c| &c.name == column);
                            write!(result, "{}|", column.unwrap().value)?;
                        }
                        SqlColumnVariant::EveryColumn => {
                            for column in &record.columns {
                                write!(result, "{}|", column.value)?;
                            }
                        }
                        _ => panic!("Invalid column variant"),
                    }
                }
                println!("{}", result.trim_end_matches('|'));
            }
        }
    }

    Ok(())
}

struct Schema {
    page_reader: PageReader,
    schema_table: SchemaTable,
}

impl<'a> Schema {
    fn new(file_name: &str) -> Result<Self> {
        let page_reader = PageReader::new(file_name)?;
        let schema_table_page = page_reader.read_page(1)?;
        let b_tree_page = BTreePage::with_offset_header(&schema_table_page, 100)?;
        let schema_table = SchemaTable::new(Schema::records_by_page(&page_reader, b_tree_page)?);

        let schema = Self {
            page_reader,
            schema_table,
        };

        Ok(schema)
    }

    fn tables(&self) -> Iter<Rc<SchemaRecord>> {
        self.schema_table.records.iter()
    }

    fn records(&'a self, sql_statement: &'a SqlStatement<'a>) -> Result<Records> {
        let table = self
            .tables()
            .find(|t| t.name == sql_statement.table_name)
            .ok_or_else(|| anyhow!("Table not found"))?;

        sql_statement.validate(table.clone())?;

        let page = self.page_reader.read_page(table.rootpage)?;
        let b_tree_page = BTreePage::new(&page)?;

        let mut records =
            Schema::records_by_page(&self.page_reader, b_tree_page)?.named(table.clone())?;

        if let Some(where_clause) = &sql_statement.where_clause {
            records = records.r#where(where_clause);
        }

        Ok(records)
    }

    fn records_by_page(page_reader: &'a PageReader, b_tree_page: BTreePage) -> Result<Records> {
        Ok(Records::new(b_tree_page, page_reader)?)
    }
}

#[derive(Debug)]
struct PageReader {
    file: File,
    page_size: usize,
}

impl PageReader {
    fn new(file_name: &str) -> Result<Self> {
        let mut file = File::open(file_name)?;
        let mut header = [0; 100];
        file.read_exact(&mut header)?;

        let page_size: usize = u16::from_be_bytes([header[16], header[17]]) as usize;
        file.seek(std::io::SeekFrom::Start(0))?;

        Ok(Self { file, page_size })
    }

    fn read_page(&self, page_num: u32) -> Result<Vec<u8>> {
        let mut file = &self.file;

        let mut page = vec![0; self.page_size];
        file.seek(std::io::SeekFrom::Start(
            (page_num as u64 - 1) * self.page_size as u64,
        ))?;
        file.read_exact(&mut page)?;

        Ok(page)
    }
}

#[derive(Debug, PartialEq)]
enum SqlColumnVariant<'a> {
    Count,
    Column(&'a str),
    EveryColumn,
}
