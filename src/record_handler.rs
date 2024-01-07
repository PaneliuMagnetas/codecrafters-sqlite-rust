use super::tokenizer::{Token, Tokenizer};
use super::BTreeLeafTableCell;

use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct SchemaRecord {
    pub r#type: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u32,
    pub sql: String,
}

impl From<BTreeLeafTableCell> for SchemaRecord {
    fn from(cell: BTreeLeafTableCell) -> Self {
        let mut cell_iter = cell.values().unwrap().into_iter();

        let r#type = match cell_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let name = match cell_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let tbl_name = match cell_iter.next() {
            Some(RecordFormat::String(s)) => s,
            _ => panic!("Invalid record format"),
        };

        let rootpage = match cell_iter.next() {
            Some(RecordFormat::Integer8(i)) => i as i64,
            Some(RecordFormat::Integer16(i)) => i as i64,
            Some(RecordFormat::Integer24(i)) => i as i64,
            Some(RecordFormat::Integer48(i)) => i as i64,
            Some(RecordFormat::Integer64(i)) => i as i64,
            _ => panic!("Invalid record format"),
        };

        let sql = match cell_iter.next() {
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
    pub fn table_column_names(&self) -> Result<Vec<String>> {
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

            if tokens.len() < 1 {
                bail!("Invalid SQL create statement");
            }

            let token = match tokens.into_iter().next() {
                Some(Token::Text(s)) => s,
                Some(Token::String(s)) => s,
                _ => bail!("Invalid SQL create statement"),
            };

            result.push(token);

            match tokenizer.next() {
                Some(Token::Punctuation(')')) => break Ok(result),
                _ => (),
            }
        }
    }

    pub fn index_columns(&self) -> Result<Vec<String>> {
        let mut tokenizer = Tokenizer::new(&self.sql);

        tokenizer.tag("CREATE")?;
        tokenizer.tag("INDEX")?;

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

        tokenizer.tag("ON")?;

        match tokenizer.next() {
            Some(Token::Text(s)) => {
                if s != self.tbl_name {
                    bail!("Invalid SQL create statement");
                }
            }
            Some(Token::String(s)) => {
                if s != self.tbl_name {
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

            if tokens.len() < 1 {
                bail!("Invalid SQL create statement");
            }

            let token = match tokens.into_iter().next() {
                Some(Token::Text(s)) => s,
                Some(Token::String(s)) => s,
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
pub struct Varint {
    pub value: i64,
    pub size: u8,
}

impl Varint {
    pub fn from(buf: &[u8]) -> (Self, &[u8]) {
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

#[derive(Debug, PartialEq, Clone)]
pub enum RecordFormat {
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
    pub fn new(payload: &[u8], value: i64) -> Result<(Self, &[u8])> {
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

impl PartialEq<Token> for RecordFormat {
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

impl From<RecordFormat> for usize {
    fn from(record: RecordFormat) -> Self {
        match record {
            RecordFormat::NULL => panic!("NULL cannot be converted to usize"),
            RecordFormat::Integer8(i) => i as usize,
            RecordFormat::Integer16(i) => i as usize,
            RecordFormat::Integer24(i) => i as usize,
            RecordFormat::Integer32(i) => i as usize,
            RecordFormat::Integer48(i) => i as usize,
            RecordFormat::Integer64(i) => i as usize,
            RecordFormat::Float64(_) => panic!("Float64 cannot be converted to usize"),
            RecordFormat::Integer0 => 0,
            RecordFormat::Integer1 => 1,
            RecordFormat::Blob(_) => panic!("Blob cannot be converted to usize"),
            RecordFormat::String(_) => panic!("String cannot be converted to usize"),
        }
    }
}

impl From<RecordFormat> for String {
    fn from(record: RecordFormat) -> Self {
        match record {
            RecordFormat::NULL => "NULL".to_string(),
            RecordFormat::Integer8(i) => i.to_string(),
            RecordFormat::Integer16(i) => i.to_string(),
            RecordFormat::Integer24(i) => i.to_string(),
            RecordFormat::Integer32(i) => i.to_string(),
            RecordFormat::Integer48(i) => i.to_string(),
            RecordFormat::Integer64(i) => i.to_string(),
            RecordFormat::Float64(f) => f.to_string(),
            RecordFormat::Integer0 => "0".to_string(),
            RecordFormat::Integer1 => "1".to_string(),
            RecordFormat::Blob(b) => format!("{:?}", b),
            RecordFormat::String(s) => s,
        }
    }
}

impl From<&RecordFormat> for String {
    fn from(record: &RecordFormat) -> Self {
        match record {
            RecordFormat::NULL => "NULL".to_string(),
            RecordFormat::Integer8(i) => i.to_string(),
            RecordFormat::Integer16(i) => i.to_string(),
            RecordFormat::Integer24(i) => i.to_string(),
            RecordFormat::Integer32(i) => i.to_string(),
            RecordFormat::Integer48(i) => i.to_string(),
            RecordFormat::Integer64(i) => i.to_string(),
            RecordFormat::Float64(f) => f.to_string(),
            RecordFormat::Integer0 => "0".to_string(),
            RecordFormat::Integer1 => "1".to_string(),
            RecordFormat::Blob(b) => format!("{:?}", b),
            RecordFormat::String(s) => s.to_string(),
        }
    }
}
