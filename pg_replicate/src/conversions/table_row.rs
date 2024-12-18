use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use postgres_protocol::types;
use thiserror::Error;
use tokio_postgres::{
    binary_copy::BinaryCopyOutRow,
    types::{FromSql, Type},
};
use trait_gen::trait_gen;

use crate::{pipeline::batching::BatchBoundary, table::ColumnSchema};

#[derive(Debug)]
pub enum Cell {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    I64(i64),
    TimeStamp(String),
    Bytes(Vec<u8>),
}

impl TryFrom<Cell> for bool {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::Bool(b) => Ok(b),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for i32 {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::I32(i) => Ok(i),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for u32 {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::I32(i) => Ok(i as u32),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for i64 {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::I64(i) => Ok(i),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for u64 {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::I64(i) => Ok(i as u64),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for String {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::String(s) => Ok(s),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for std::borrow::Cow<'static, str> {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::String(s) => Ok(std::borrow::Cow::Owned(s)),
            _ => Err(CellConversionError),
        }
    }
}

impl TryFrom<Cell> for DateTime<Utc> {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::TimeStamp(s) => Ok(s.parse().map_err(|_| CellConversionError)?),
            _ => Err(CellConversionError),
        }
    }
}

#[trait_gen(T -> bool, i32, u32, i64, u64, String, Vec<u8>, DateTime<Utc>)]
impl TryFrom<Cell> for Option<T> {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::Null => Ok(None),
            _ => T::try_from(cell).map(Some),
        }
    }
}

impl TryFrom<Cell> for Vec<u8> {
    type Error = CellConversionError;

    fn try_from(cell: Cell) -> Result<Self, CellConversionError> {
        match cell {
            Cell::Bytes(b) => Ok(b),
            _ => Err(CellConversionError),
        }
    }
}

#[derive(Debug, Error)]
#[error("cell conversion error")]
pub struct CellConversionError;

#[derive(Debug)]
pub struct TableRow {
    pub values: Vec<Cell>,
}

impl BatchBoundary for TableRow {
    fn is_last_in_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Error)]
pub enum TableRowConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(Type),

    #[error("failed to get timestamp nanos from {0}")]
    NoTimestampNanos(DateTime<Utc>),
}

pub struct TableRowConverter;

/// A wrapper type over Vec<u8> to help implement the FromSql trait.
/// The wrapper is needed to avoid Rust's trait coherence rules. i.e.
/// one of the trait or the implementing type must be part of the
/// current crate.
///
/// This type is useful in retriveing bytes from the Postgres wire
/// protocol for the fallback case of unsupported type.
struct VecWrapper(Vec<u8>);

impl<'a> FromSql<'a> for VecWrapper {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<VecWrapper, Box<dyn std::error::Error + Sync + Send>> {
        let v = types::bytea_from_sql(raw).to_owned();
        Ok(VecWrapper(v))
    }

    /// Because of the fallback nature of this impl, we accept all types here
    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(VecWrapper(vec![]))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }
}

impl TableRowConverter {
    fn get_cell_value(
        row: &BinaryCopyOutRow,
        column_schema: &ColumnSchema,
        i: usize,
    ) -> Result<Cell, TableRowConversionError> {
        match column_schema.typ {
            Type::BOOL => {
                let val = if column_schema.nullable {
                    match row.try_get::<bool>(i) {
                        Ok(b) => Cell::Bool(b),
                        //TODO: Only return null if the error is WasNull from tokio_postgres crate
                        Err(_) => Cell::Null,
                    }
                } else {
                    let val = row.get::<bool>(i);
                    Cell::Bool(val)
                };
                Ok(val)
            }
            // Type::BYTEA => {
            //     let bytes = row.get(i);
            //     Ok(Value::Bytes(bytes))
            // }
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                let val = if column_schema.nullable {
                    match row.try_get::<&str>(i) {
                        Ok(s) => Cell::String(s.to_string()),
                        //TODO: Only return null if the error is WasNull from tokio_postgres crate
                        Err(_) => Cell::Null,
                    }
                } else {
                    let val = row.get::<&str>(i);
                    Cell::String(val.to_string())
                };
                Ok(val)
            }
            // Type::JSON | Type::JSONB => {
            //     let val = row.get::<serde_json::Value>(i);
            //     let val = json_to_cbor_value(&val);
            //     Ok(val)
            // }
            Type::INT2 => {
                let val = if column_schema.nullable {
                    match row.try_get::<i16>(i) {
                        Ok(i) => Cell::I16(i),
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<i16>(i);
                    Cell::I16(val)
                };
                Ok(val)
            }
            Type::INT4 => {
                let val = if column_schema.nullable {
                    match row.try_get::<i32>(i) {
                        Ok(i) => Cell::I32(i),
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<i32>(i);
                    Cell::I32(val)
                };
                Ok(val)
            }
            Type::INT8 => {
                let val = if column_schema.nullable {
                    match row.try_get::<i64>(i) {
                        Ok(i) => Cell::I64(i),
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<i64>(i);
                    Cell::I64(val)
                };
                Ok(val)
            }
            Type::TIMESTAMP => {
                let val = if column_schema.nullable {
                    match row.try_get::<NaiveDateTime>(i) {
                        Ok(s) => {
                            let s = s.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                            Cell::TimeStamp(s.to_string())
                        }
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<NaiveDateTime>(i);
                    let val = val.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                    Cell::TimeStamp(val)
                };
                Ok(val)
            }
            Type::TIMESTAMPTZ => {
                let val = if column_schema.nullable {
                    match row.try_get::<DateTime<FixedOffset>>(i) {
                        Ok(s) => {
                            let s = s.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                            Cell::TimeStamp(s)
                        }
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<DateTime<FixedOffset>>(i);
                    let val = val.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                    Cell::TimeStamp(val)
                };
                Ok(val)
            }
            _ => {
                let val = if column_schema.nullable {
                    match row.try_get::<VecWrapper>(i) {
                        Ok(v) => {
                            // CR alee: not convinced this is totally correct
                            if v.0.is_empty() {
                                Cell::Null
                            } else {
                                Cell::Bytes(v.0)
                            }
                        }
                        Err(_) => {
                            //TODO: Only return null if the error is WasNull from tokio_postgres crate
                            Cell::Null
                        }
                    }
                } else {
                    let val = row.get::<VecWrapper>(i);
                    Cell::Bytes(val.0)
                };
                Ok(val)
            }
        }
    }

    pub fn try_from(
        row: &tokio_postgres::binary_copy::BinaryCopyOutRow,
        column_schemas: &[crate::table::ColumnSchema],
    ) -> Result<TableRow, TableRowConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());
        for (i, column_schema) in column_schemas.iter().enumerate() {
            let value = Self::get_cell_value(row, column_schema, i)?;
            values.push(value);
        }

        Ok(TableRow { values })
    }
}
