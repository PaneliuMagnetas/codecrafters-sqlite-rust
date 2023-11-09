mod tokenizer;

use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::fs::File;
use std::io::{Read, Seek};

use crate::tokenizer::{Token, Tokenizer};

#[derive(Debug)]
enum BTreeCell {
    // InteriorIndexCell(BTreeInteriorIndexCell),
    // InteriorTableCell(BTreeInteriorTableCell),
    // LeafIndexCell(BTreeLeafIndexCell),
    LeafTableCell(BTreeLeafTableCell),
}

#[allow(dead_code)]
#[derive(Debug)]
struct BTreeLeafTableCell {
    payload_size: Varint,
    row_id: Varint,
    payload: Record,
    first_overflow_page: u32,
}

impl BTreeLeafTableCell {}

#[derive(Debug)]
struct Varint {
    value: i64,
    size: u8,
}

#[derive(Debug)]
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

    fn matches(&self, token: &Token) -> Result<bool> {
        match token {
            Token::Number(n) => match self {
                RecordFormat::Integer8(v) => Ok(*n == *v as i64),
                RecordFormat::Integer16(v) => Ok(*n == *v as i64),
                RecordFormat::Integer24(v) => Ok(*n == *v as i64),
                RecordFormat::Integer32(v) => Ok(*n == *v as i64),
                RecordFormat::Integer48(v) => Ok(*n == *v),
                RecordFormat::Integer64(v) => Ok(*n == *v),
                _ => bail!("Mismatched record format"),
            },
            Token::Text(t) => match self {
                RecordFormat::NULL => Ok(*t == "NULL"),
                _ => bail!("Mismatched record format"),
            },
            Token::String(s) => match self {
                RecordFormat::String(v) => Ok(s == v),
                _ => bail!("Mismatched record format"),
            },
            _ => bail!("Mismatched record format"),
        }
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
    body: Vec<RecordFormat>,
}

impl Record {
    fn new(payload: &[u8]) -> Result<Self> {
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

        Ok(Record { body })
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
    // InteriorTablePage = 0x05,
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

impl BTreePage {
    fn new(page: &[u8]) -> Result<Self> {
        let mut b_tree_page = Self::read_header(page)?;
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
                BTreePageType::LeafTablePage => {
                    let (payload_size, page_slice) = Varint::from(page_slice);
                    let (row_id, page_slice) = Varint::from(page_slice);

                    let payload_size_val = payload_size.value as usize;
                    BTreeCell::LeafTableCell(BTreeLeafTableCell {
                        payload_size,
                        row_id,
                        payload: Record::new(&page_slice[..(payload_size_val as usize)])?,
                        first_overflow_page: 0,
                    })
                }
            });
        }

        self.cells = cells;

        Ok(())
    }

    fn read_header(page: &[u8]) -> Result<Self> {
        let page_type = match page[0] {
            // 0x02 => BTreePageType::InteriorIndexPage,
            // 0x05 => BTreePageType::InteriorTablePage,
            // 0x0a => BTreePageType::LeafIndexPage,
            0x0d => BTreePageType::LeafTablePage,
            _ => bail!("Invalid page type"),
        };

        let first_freeblock_offset = u16::from_be_bytes([page[1], page[2]]);
        let num_cells = u16::from_be_bytes([page[3], page[4]]);
        let cell_content_area = u16::from_be_bytes([page[5], page[6]]);
        let fragment_bytes = page[7];
        // if page.page_type == BTreePageType::InteriorIndexPage
        //     || page.page_type == BTreePageType::InteriorTablePage
        // {
        //     file.read_exact(&mut page.right_most_pointer.to_be_bytes())?;
        // }

        let mut cell_pointers = Vec::new();

        let cell_pointer_size = num_cells as usize * 2 + 8;

        for chunk in page[8..cell_pointer_size].chunks(2) {
            cell_pointers.push(u16::from_be_bytes([chunk[0], chunk[1]]));
        }

        Ok(BTreePage {
            page_type,
            first_freeblock_offset,
            num_cells,
            cell_content_area,
            fragment_bytes,
            right_most_pointer: 0,
            cell_pointers,
            cells: vec![],
        })
    }
}

#[derive(Debug)]
struct SchemaTable {
    records: HashMap<String, SchemaRecord>,
}

impl SchemaTable {
    fn new(b_tree_page: BTreePage) -> Result<Self> {
        match b_tree_page.page_type {
            BTreePageType::LeafTablePage => Ok(SchemaTable {
                records: b_tree_page
                    .cells
                    .into_iter()
                    .sorted_by(|a, b| match (a, b) {
                        (BTreeCell::LeafTableCell(a), BTreeCell::LeafTableCell(b)) => {
                            a.row_id.value.cmp(&b.row_id.value)
                        }
                    })
                    .map(|c| match c {
                        BTreeCell::LeafTableCell(c) => {
                            eprintln!("{:?}", c.row_id.value);
                            let payload: SchemaRecord = c.payload.into();
                            (payload.tbl_name.clone(), payload)
                        }
                    })
                    .collect::<HashMap<String, SchemaRecord>>(),
            }),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct SchemaRecord {
    r#type: String,
    name: String,
    tbl_name: String,
    rootpage: i64,
    sql: String,
}

impl From<Record> for SchemaRecord {
    fn from(record: Record) -> Self {
        let mut record_body_iter = record.body.iter();

        let r#type = match record_body_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let name = match record_body_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let tbl_name = match record_body_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let rootpage = match record_body_iter.next() {
            Some(RecordFormat::Integer8(i)) => *i as i64,
            Some(RecordFormat::Integer16(i)) => *i as i64,
            Some(RecordFormat::Integer24(i)) => *i as i64,
            Some(RecordFormat::Integer48(i)) => *i as i64,
            Some(RecordFormat::Integer64(i)) => *i as i64,
            _ => panic!("Invalid record format"),
        };

        let sql = match record_body_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        SchemaRecord {
            r#type: r#type.to_string(),
            name: name.to_string(),
            tbl_name: tbl_name.to_string(),
            rootpage,
            sql: sql.to_string(),
        }
    }
}

impl SchemaRecord {
    fn get_columns(&self) -> Result<HashMap<String, usize>> {
        let mut tokenizer = Tokenizer::new(&self.sql);

        tokenizer.tag("CREATE")?;
        tokenizer.tag("TABLE")?;
        tokenizer.tag(&self.tbl_name)?;

        tokenizer.tag("(")?;

        let mut result = HashMap::new();

        let mut i: usize = 0;
        loop {
            let tokens = tokenizer
                .take_while(|t| *t != Token::Punctuation(',') && *t != Token::Punctuation(')'));

            let token = match tokens[0] {
                Token::Text(s) => s,
                _ => bail!("Invalid SQL create statement"),
            };

            result.insert(String::from(token), i);
            i += 1;

            match tokenizer.next() {
                Some(Token::Punctuation(')')) => break Ok(result),
                _ => (),
            }
        }
    }
}

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
                    Token::Punctuation(',') => continue,
                    Token::Text("from") => break,
                    Token::Text("count") => {
                        tokenizer.tag("(")?;
                        if let Some(_) = tokenizer.next() {
                            result = Ok(SqlColumnVariant::Count);
                        }
                        tokenizer.tag(")")?;
                    }
                    Token::Text(column) => result = Ok(SqlColumnVariant::Column(column)),
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
}

struct SqlWhereClause<'a> {
    columns: Vec<SqlWhereColumn<'a>>,
}

#[allow(dead_code)]
struct SqlWhereColumn<'a> {
    column: &'a str,
    operator: SqlOperator,
    value: Token<'a>,
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

    fn matches(&self, b_tree_cell: &BTreeCell, columns: &HashMap<String, usize>) -> Result<bool> {
        for wc in self.columns.iter() {
            let column_index = match columns.get(wc.column) {
                Some(i) => i,
                None => return Err(anyhow!("Column {} not found", wc.column,)),
            };

            match b_tree_cell {
                BTreeCell::LeafTableCell(leaf_table_cell) => {
                    if !leaf_table_cell.payload.body[*column_index].matches(&wc.value)? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }
}

enum SqlOperator {
    Equal,
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];

    let mut file = File::open(&args[1])?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let page_size: usize = u16::from_be_bytes([header[16], header[17]]) as usize;
    file.seek(std::io::SeekFrom::Start(0))?;

    let mut schema_table_page = vec![0; page_size];
    file.read_exact(&mut schema_table_page)?;
    let b_tree_page = BTreePage::with_offset_header(schema_table_page.as_slice(), 100)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", page_size);
            println!("number of tables: {}", b_tree_page.num_cells);
        }
        ".tables" => {
            let mut result = String::new();
            let schema_table = SchemaTable::new(b_tree_page)?;

            for (tbl_name, _) in schema_table.records {
                result += &format!("{} ", tbl_name);
            }

            println!("{}", result.trim_end());
        }
        x => {
            let sql_statement = SqlStatement::new(x)?;
            let schema_table = SchemaTable::new(b_tree_page)?;

            let schema_record = schema_table
                .records
                .get(sql_statement.table_name)
                .unwrap_or_else(|| {
                    eprintln!("Table {} not found", sql_statement.table_name);
                    std::process::exit(1)
                });

            let column_names = schema_record.get_columns()?;

            file.seek(std::io::SeekFrom::Start(
                (schema_record.rootpage as u64 - 1) * page_size as u64,
            ))?;

            let mut buf = vec![0; page_size];
            file.read_exact(&mut buf)?;

            let b_tree_page = BTreePage::new(&buf)?;

            if sql_statement.column_variants.len() == 1 {
                if let SqlColumnVariant::Count = sql_statement.column_variants[0] {
                    println!("{}", b_tree_page.num_cells);
                    return Ok(());
                }
            }

            let flag = sql_statement.where_clause.is_some();

            for cell in b_tree_page.cells {
                match &cell {
                    BTreeCell::LeafTableCell(leaf_table_cell) => {
                        if flag {
                            if !sql_statement
                                .where_clause
                                .as_ref()
                                .unwrap()
                                .matches(&cell, &column_names)?
                            {
                                continue;
                            }
                        }

                        let mut result = String::new();

                        for column_variant in &sql_statement.column_variants {
                            match column_variant {
                                SqlColumnVariant::Count => {
                                    result += &format!("{}|", b_tree_page.num_cells);
                                }
                                SqlColumnVariant::Column(column_name) => {
                                    let column_index = match column_names.get(*column_name) {
                                        Some(i) => i,
                                        None => {
                                            eprintln!(
                                                "Column {} not found in table {}",
                                                column_name, sql_statement.table_name
                                            );
                                            std::process::exit(1)
                                        }
                                    };

                                    result += &format!(
                                        "{}|",
                                        leaf_table_cell.payload.body[*column_index]
                                    );
                                }
                                SqlColumnVariant::EveryColumn => {
                                    for column in &leaf_table_cell.payload.body {
                                        result += &format!("{}|", column);
                                    }
                                }
                            }
                        }

                        println!("{}", result.trim_end_matches('|'));
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
enum SqlColumnVariant<'a> {
    Count,
    Column(&'a str),
    EveryColumn,
}
