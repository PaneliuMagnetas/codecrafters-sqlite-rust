use super::record_handler::{RecordFormat, Varint};
use super::CellIterator;

use anyhow::{bail, Result};
use std::fs::File;
use std::io::{Read, Seek};

#[derive(Debug)]
pub struct PageReader {
    file: File,
    page_size: u16,
}

impl PageReader {
    pub fn new(file_name: &str) -> Result<Self> {
        let mut file = File::open(file_name)?;
        let mut header = [0; 100];
        file.read_exact(&mut header)?;

        let page_size = u16::from_be_bytes([header[16], header[17]]);
        file.seek(std::io::SeekFrom::Start(0))?;

        Ok(Self { file, page_size })
    }

    pub fn read_page(&mut self, page_number: u32) -> Result<BTreePage> {
        let mut page = vec![0; self.page_size as usize];
        self.file.seek(std::io::SeekFrom::Start(
            ((page_number - 1) * self.page_size as u32) as u64,
        ))?;
        self.file.read_exact(&mut page)?;

        if page_number == 1 {
            Ok(BTreePage::with_offset_header(page, 100)?)
        } else {
            Ok(BTreePage::new(page)?)
        }
    }

    pub fn page_size(&self) -> u16 {
        self.page_size
    }
}

#[derive(Debug)]
pub enum BTreeCell {
    InteriorIndexCell(BTreeInteriorIndexCell),
    InteriorTableCell(BTreeInteriorTableCell),
    LeafIndexCell(BTreeLeafIndexCell),
    LeafTableCell(BTreeLeafTableCell),
}

#[derive(Debug)]
pub struct BTreeInteriorIndexCell {
    pub left_child_page: u32,
    //payload_size: Varint,
    payload: Vec<u8>,
    //first_overflow_page: u32,
}

impl BTreeInteriorIndexCell {
    pub fn values(&self) -> Result<Vec<RecordFormat>> {
        values(&self.payload)
    }
}

#[derive(Debug)]
pub struct BTreeInteriorTableCell {
    pub left_child_page: u32,
    pub row_id: Varint,
}

#[derive(Debug)]
pub struct BTreeLeafTableCell {
    //payload_size: Varint,
    pub row_id: Varint,
    payload: Vec<u8>,
    //first_overflow_page: u32,
}

pub fn values(payload: &Vec<u8>) -> Result<Vec<RecordFormat>> {
    let mut header = Vec::new();
    let mut values = Vec::new();
    let mut payload = payload.as_slice();

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
        values.push(record_format);
    }

    Ok(values)
}

impl BTreeLeafTableCell {
    pub fn values(&self) -> Result<Vec<RecordFormat>> {
        values(&self.payload)
    }
}

#[derive(Debug)]
pub struct BTreeLeafIndexCell {
    //payload_size: Varint,
    payload: Vec<u8>,
    //first_overflow_page: u32,
}

impl BTreeLeafIndexCell {
    pub fn values(&self) -> Result<Vec<RecordFormat>> {
        values(&self.payload)
    }
}

#[derive(Debug)]
pub struct BTreePage {
    pub page_type: BTreePageType,
    //first_freeblock_offset: u16,
    pub num_cells: u16,
    cell_content_area: u16,
    //fragment_bytes: u8,
    pub right_most_pointer: Option<u32>,
    cell_pointers: Vec<u16>,
    cell_content: Vec<u8>,
}

impl<'a> BTreePage {
    fn new(page: Vec<u8>) -> Result<Self> {
        let b_tree_page = Self::read_header(&page)?;

        Ok(b_tree_page)
    }

    fn with_offset_header(page: Vec<u8>, offset: usize) -> Result<Self> {
        let mut b_tree_page = Self::read_header(&page[offset..])?;
        b_tree_page.cell_content = page[b_tree_page.cell_content_area as usize..].to_vec();

        Ok(b_tree_page)
    }

    pub fn cells(&self) -> Result<Vec<BTreeCell>> {
        let mut cells = Vec::new();
        for cell_pointer in &self.cell_pointers {
            let page_slice = &self.cell_content[*cell_pointer as usize..];
            cells.push(match self.page_type {
                BTreePageType::InteriorIndexPage => {
                    let left_child_page = u32::from_be_bytes(page_slice[..4].try_into()?);
                    let page_slice = &page_slice[4..];
                    let (payload_size, page_slice) = Varint::from(page_slice);

                    let payload_size_val = payload_size.value as usize;
                    BTreeCell::InteriorIndexCell(BTreeInteriorIndexCell {
                        left_child_page,
                        //payload_size,
                        payload: page_slice[..(payload_size_val as usize)].to_vec(),
                        //first_overflow_page: 0,
                    })
                }
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
                        //payload_size,
                        row_id,
                        payload: page_slice[..(payload_size_val as usize)].to_vec(),
                        //first_overflow_page: 0,
                    })
                }
                BTreePageType::LeafIndexPage => {
                    let (payload_size, page_slice) = Varint::from(page_slice);

                    let payload_size_val = payload_size.value as usize;
                    BTreeCell::LeafIndexCell(BTreeLeafIndexCell {
                        //payload_size,
                        payload: page_slice[..(payload_size_val as usize)].to_vec(),
                        //first_overflow_page: 0,
                    })
                }
            });
        }

        Ok(cells)
    }

    fn read_header(page: &[u8]) -> Result<Self> {
        let page_type = match page[0] {
            0x02 => BTreePageType::InteriorIndexPage,
            0x05 => BTreePageType::InteriorTablePage,
            0x0a => BTreePageType::LeafIndexPage,
            0x0d => BTreePageType::LeafTablePage,
            _ => bail!("Invalid page type {}", page[0]),
        };

        let mut header_len = 8;
        let _first_freeblock_offset = u16::from_be_bytes([page[1], page[2]]);
        let num_cells = u16::from_be_bytes([page[3], page[4]]);
        let cell_content_area = u16::from_be_bytes([page[5], page[6]]);
        let _fragment_bytes = page[7];
        let mut right_most_pointer = None;
        if page_type == BTreePageType::InteriorIndexPage
            || page_type == BTreePageType::InteriorTablePage
        {
            right_most_pointer = Some(u32::from_be_bytes([page[8], page[9], page[10], page[11]]));
            header_len += 4;
        }

        let mut cell_pointers = Vec::new();

        let cell_pointer_size = num_cells as usize * 2 + header_len;

        for chunk in page[header_len..cell_pointer_size].chunks(2) {
            cell_pointers.push(u16::from_be_bytes([chunk[0], chunk[1]]) - cell_content_area);
        }

        Ok(BTreePage {
            page_type,
            //first_freeblock_offset,
            num_cells,
            cell_content_area,
            //fragment_bytes,
            right_most_pointer,
            cell_pointers,
            cell_content: page[cell_content_area as usize..].to_vec(),
        })
    }

    pub fn iter(self) -> Result<CellIterator> {
        CellIterator::new(self)
    }
}

#[derive(PartialEq, Debug)]
pub enum BTreePageType {
    InteriorIndexPage = 0x02,
    InteriorTablePage = 0x05,
    LeafIndexPage = 0x0a,
    LeafTablePage = 0x0d,
}
