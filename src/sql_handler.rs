use anyhow::{anyhow, bail, Result};

use super::record_handler::SchemaRecord;
use super::tokenizer::{Token, Tokenizer};
use super::Record;

#[derive(Debug)]
pub struct SqlStatement {
    column_variants: Vec<SqlColumnVariant>,
    pub table_name: String,
    pub where_clause: Option<SqlWhereClause>,
}

impl SqlStatement {
    pub fn new(sql: &str) -> Result<Self> {
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

    pub fn map(
        &self,
        table_schema: &SchemaRecord,
        index_schema: Option<&SchemaRecord>,
    ) -> Result<MappedSqlStatement> {
        let column_names = table_schema.table_column_names()?;

        let mut column_variants = Vec::new();

        for c in &self.column_variants {
            match c {
                SqlColumnVariant::Column(col) => {
                    let column = column_names
                        .iter()
                        .position(|s| s == col)
                        .ok_or_else(|| anyhow!("Invalid column name {} in select clause", col))?;

                    column_variants.push(MappedSqlColumnVariant::Column(column));
                }
                SqlColumnVariant::EveryColumn => {
                    column_variants.push(MappedSqlColumnVariant::EveryColumn);
                }
                SqlColumnVariant::Count => {
                    column_variants.push(MappedSqlColumnVariant::Count);
                }
            }
        }

        let where_clause = if let Some(where_clause) = &self.where_clause {
            Some(where_clause.map(table_schema, index_schema)?)
        } else {
            None
        };

        Ok(MappedSqlStatement {
            column_variants,
            where_clause,
        })
    }
}

#[derive(Debug)]
pub struct SqlWhereClause {
    columns: Vec<SqlWhereColumn>,
}

#[derive(Debug)]
pub struct SqlWhereColumn {
    column: String,
    operator: SqlOperator,
    value: String,
}

impl SqlWhereClause {
    fn new(sql: &str) -> Result<Self> {
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
                value: String::from(value),
            });
        }

        Ok(SqlWhereClause { columns })
    }

    fn map(
        &self,
        table_schema: &SchemaRecord,
        index_schema: Option<&SchemaRecord>,
    ) -> Result<MappedSqlWhereClause> {
        let column_names = table_schema.table_column_names()?;

        let mut columns = {
            let mut columns = Vec::new();

            for column in &self.columns {
                let position = column_names
                    .iter()
                    .position(|s| s == &column.column)
                    .ok_or_else(|| {
                        anyhow!("Invalid column name {} in where clause", column.column)
                    })?;

                columns.push(MappedSqlWhereColumn {
                    column: position,
                    operator: column.operator,
                    value: column.value.clone(),
                });
            }

            columns
        };

        let mut keys = Vec::new();

        if let Some(index_schema) = index_schema {
            index_schema
                .index_columns()?
                .iter()
                .map(|c| column_names.iter().position(|s| s == c).unwrap())
                .for_each(|c| {
                    let index = columns.iter().position(|cc| cc.column == c);

                    if let Some(index) = index {
                        keys.push(columns.remove(index));
                    }
                });
        }

        Ok(MappedSqlWhereClause { columns, keys })
    }
}

#[derive(Debug, Copy, Clone)]
enum SqlOperator {
    Equal,
}

#[derive(Debug)]
pub struct MappedSqlWhereColumn {
    pub column: usize,
    operator: SqlOperator,
    pub value: String,
}

#[derive(Debug)]
pub struct MappedSqlWhereClause {
    pub columns: Vec<MappedSqlWhereColumn>,
    pub keys: Vec<MappedSqlWhereColumn>,
}

impl MappedSqlWhereClause {
    pub fn matches(&self, record: &Record) -> bool {
        for c in &self.columns {
            let column_value = &record.values[c.column];
            let value = &c.value;

            match c.operator {
                SqlOperator::Equal => {
                    if String::from(column_value) != *value {
                        return false;
                    }
                }
            }
        }

        true
    }

    pub fn keys(&self) -> Vec<String> {
        let mut keys = Vec::new();

        for c in &self.keys {
            keys.push(c.value.clone());
        }

        keys.iter().map(|s| s.clone()).collect()
    }
}

#[derive(Debug)]
pub struct MappedSqlStatement {
    pub column_variants: Vec<MappedSqlColumnVariant>,
    pub where_clause: Option<MappedSqlWhereClause>,
}

#[derive(Debug, PartialEq)]
pub enum SqlColumnVariant {
    Count,
    Column(String),
    EveryColumn,
}

#[derive(Debug, PartialEq)]
pub enum MappedSqlColumnVariant {
    Count,
    Column(usize),
    EveryColumn,
}
