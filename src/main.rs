use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

#[derive(Debug)]
enum BTreeCell {
    // InteriorIndexCell(BTreeInteriorIndexCell),
    // InteriorTableCell(BTreeInteriorTableCell),
    // LeafIndexCell(BTreeLeafIndexCell),
    LeafTableCell(BTreeLeafTableCell),
}

#[derive(Debug)]
struct BTreeLeafTableCell {
    payload_size: Varint,
    row_id: Varint,
    payload: Record,
    first_overflow_page: u32,
}

#[derive(Debug)]
struct BTreePageHeader {
    page_type: BTreePageType,
    first_freeblock_offset: u16,
    num_cells: u16,
    cell_content_area: u16,
    fragment_bytes: u8,
    right_most_pointer: u32,
    cell_pointers: Vec<u16>,
}

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
    fn new(file: &mut File, v: &Varint) -> Result<Self> {
        match v.value {
            0 => Ok(RecordFormat::NULL),
            1 => {
                let mut buf = [0; 1];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Integer8(buf[0] as i8))
            }
            2 => {
                let mut buf = [0; 2];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Integer16(i16::from_be_bytes(buf)))
            }
            3 => {
                let mut buf = [0; 3];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Integer24(i32::from_be_bytes([
                    0, buf[0], buf[1], buf[2],
                ])))
            }
            4 => {
                let mut buf = [0; 4];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Integer32(i32::from_be_bytes(buf)))
            }
            5 => {
                let mut buf = [0; 6];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Integer48(i64::from_be_bytes([
                    0, 0, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5],
                ])))
            }
            6 => {
                let mut buf = [0; 8];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Integer64(i64::from_be_bytes(buf)))
            }
            7 => {
                let mut buf = [0; 8];
                file.read_exact(&mut buf)?;
                Ok(RecordFormat::Float64(f64::from_be_bytes(buf)))
            }
            8 => Ok(RecordFormat::Integer0),
            9 => Ok(RecordFormat::Integer1),
            10 | 11 => bail!("Invalid record format"),
            _ => {
                if v.value % 2 == 0 {
                    let mut buf = vec![0; ((v.value - 12) / 2) as usize];
                    file.read_exact(&mut buf)?;
                    Ok(RecordFormat::Blob(buf))
                } else {
                    let mut buf = vec![0; ((v.value - 13) / 2) as usize];
                    file.read_exact(&mut buf)?;
                    Ok(RecordFormat::String(String::from_utf8(buf)?))
                }
            }
        }
    }
}

#[derive(Debug)]
struct Record {
    body: Vec<RecordFormat>,
}

impl Record {
    fn new(file: &mut File) -> Result<Self> {
        let mut header = Vec::new();
        let mut body = Vec::new();

        let header_size_varint = Varint::from_file(file)?;
        let mut header_size = header_size_varint.value - header_size_varint.size as i64;

        while header_size > 0 {
            let varint = Varint::from_file(file)?;
            header_size -= varint.size as i64;
            header.push(varint);
        }

        for v in &header {
            let record_format = RecordFormat::new(file, v)?;
            body.push(record_format);
        }

        Ok(Record { body })
    }
}

impl Varint {
    fn from_file(file: &mut File) -> Result<Self> {
        let mut byte = [0; 1];
        let mut result = 0;
        let mut size = 0;

        loop {
            file.read_exact(&mut byte)?;
            result = (result << 7) | (byte[0] & 0x7f) as i64;
            size += 1;

            if byte[0] & 0x80 == 0 {
                break;
            }
        }

        Ok(Varint {
            value: result,
            size,
        })
    }
}

#[derive(PartialEq, Debug)]
enum BTreePageType {
    // InteriorIndexPage = 0x02,
    // InteriorTablePage = 0x05,
    // LeafIndexPage = 0x0a,
    LeafTablePage = 0x0d,
    TBD = 0x00,
}

impl BTreePageType {
    fn to_be_bytes(&self) -> [u8; 1] {
        match self {
            // BTreePageType::InteriorIndexPage => [0x02],
            // BTreePageType::InteriorTablePage => [0x05],
            // BTreePageType::LeafIndexPage => [0x0a],
            BTreePageType::LeafTablePage => [0x0d],
            BTreePageType::TBD => [0; 1],
        }
    }
}

impl BTreePageHeader {
    fn new(file: &mut File) -> Result<Self> {
        let mut page = BTreePageHeader {
            page_type: BTreePageType::TBD,
            first_freeblock_offset: 0,
            num_cells: 0,
            cell_content_area: 0,
            fragment_bytes: 0,
            right_most_pointer: 0,
            cell_pointers: Vec::new(),
        };

        let mut buf = [0; 8];

        file.read_exact(&mut buf)?;
        page.page_type = match buf[0] {
            // 0x02 => BTreePageType::InteriorIndexPage,
            // 0x05 => BTreePageType::InteriorTablePage,
            // 0x0a => BTreePageType::LeafIndexPage,
            0x0d => BTreePageType::LeafTablePage,
            _ => bail!("Invalid page type"),
        };

        page.first_freeblock_offset = u16::from_be_bytes([buf[1], buf[2]]);
        page.num_cells = u16::from_be_bytes([buf[3], buf[4]]);
        page.cell_content_area = u16::from_be_bytes([buf[5], buf[6]]);
        page.fragment_bytes = buf[7];
        // if page.page_type == BTreePageType::InteriorIndexPage
        //     || page.page_type == BTreePageType::InteriorTablePage
        // {
        //     file.read_exact(&mut page.right_most_pointer.to_be_bytes())?;
        // }

        for _ in 0..page.num_cells {
            let mut cell_buf = [0; 2];
            file.read_exact(&mut cell_buf)?;
            page.cell_pointers.push(u16::from_be_bytes(cell_buf));
        }

        Ok(page)
    }
}

struct SchemaTable {
    records: Vec<SchemaRecord>,
}

impl SchemaTable {
    fn new(file: &mut File, b_tree_page_header: BTreePageHeader) -> Result<Self> {
        let mut cells = read_cells(file, b_tree_page_header)?;
        cells.sort_by(|a, b| a.row_id.value.cmp(&b.row_id.value));

        Ok(SchemaTable {
            records: cells
                .into_iter()
                .map(|c| c.payload.into())
                .collect::<Vec<SchemaRecord>>(),
        })
    }
}

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

    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

    let b_tree_page_header = BTreePageHeader::new(&mut file)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", page_size);
            println!("number of tables: {}", b_tree_page_header.num_cells);
        }
        ".tables" => {
            let schema_table = SchemaTable::new(&mut file, b_tree_page_header)?;
            let mut result = String::new();

            for record in schema_table.records {
                result += &format!("{} ", record.name);
            }

            println!("{}", result.trim_end());
        }
        x => {
            let table = x.split(' ').last().unwrap();
            let schema_table = SchemaTable::new(&mut file, b_tree_page_header)?;

            let page = schema_table
                .records
                .iter()
                .find(|r| r.name == table)
                .unwrap()
                .rootpage;

            file.seek(std::io::SeekFrom::Start(page as u64 * page_size as u64))?;
            let b_tree_page_header = BTreePageHeader::new(&mut file)?;

            println!("{}", b_tree_page_header.num_cells);
        }
    }

    Ok(())
}

fn read_cells(
    file: &mut File,
    b_tree_page_header: BTreePageHeader,
) -> Result<Vec<BTreeLeafTableCell>> {
    let mut cells = Vec::new();

    for cell_pointer in b_tree_page_header.cell_pointers {
        file.seek(std::io::SeekFrom::Start(cell_pointer as u64))?;
        cells.push(read_cell(file)?);
    }

    Ok(cells)
}

fn read_cell(file: &mut File) -> Result<BTreeLeafTableCell> {
    let payload_size = Varint::from_file(file)?;
    let row_id = Varint::from_file(file)?;

    Ok(BTreeLeafTableCell {
        payload_size,
        row_id,
        payload: Record::new(file)?,
        first_overflow_page: 0,
    })
}
