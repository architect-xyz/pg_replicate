use std::fmt::Debug;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use derive_more::{TryInto, TryIntoError};
use numeric::PgNumeric;
use trait_gen::trait_gen;
use uuid::Uuid;

pub mod bool;
pub mod cdc_event;
pub mod hex;
pub mod numeric;
pub mod table_row;
pub mod text;

#[derive(Debug, Clone, TryInto)]
pub enum Cell {
    #[try_into(ignore)]
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    Time(NaiveTime),
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Array(ArrayCell),
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<Cell> for rust_decimal::Decimal {
    type Error = &'static str;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Numeric(PgNumeric::Value(decimal)) => Ok(decimal),
            Cell::Numeric(PgNumeric::NaN) => Err("NaN cannot be converted to rust_decimal::Decimal"),
            Cell::Numeric(PgNumeric::PositiveInf) => Err("Infinity cannot be converted to rust_decimal::Decimal"),
            Cell::Numeric(PgNumeric::NegativeInf) => Err("NegInfinity cannot be converted to rust_decimal::Decimal"),
            _ => Err("only Numeric can be converted to rust_decimal::Decimal"),
        }
    }
}

// TODO: why can't cfg(attr(..), trait_gen(..)) work?
#[cfg(feature = "rust_decimal")]
impl TryFrom<Cell> for Option<rust_decimal::Decimal> {
    type Error = &'static str;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(None),
            _ => TryInto::<rust_decimal::Decimal>::try_into(cell).map(Some)
        }
    }
}

#[trait_gen(T -> 
    bool, String, i16, i32, u32, i64, f32, f64, PgNumeric, 
    NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>,
    Uuid, serde_json::Value, Vec<u8>
)]
impl TryFrom<Cell> for Option<T> {
    type Error = TryIntoError<Cell>;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(None),
            _ => T::try_from(cell).map(Some)
        }
    }
}

#[trait_gen(T -> 
    bool, String, i16, i32, u32, i64, f32, f64, PgNumeric, 
    NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>,
    Uuid, serde_json::Value, Vec<u8>
)]
#[cfg_attr(feature = "rust_decimal", trait_gen(T -> rust_decimal::Decimal))]
impl TryFrom<Cell> for Vec<Option<T>> {
    type Error = &'static str; 

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Array(array_cell) => {
                TryInto::<Vec<Option<T>>>::try_into(array_cell)
                    .map_err(|_| "type conversion failed")
            }
            _ => Err("Only ArrayCell can be converted to Vec<Option<${T}>>"),
        }
    }
}

#[trait_gen(T -> 
    bool, String, i16, i32, u32, i64, f32, f64, PgNumeric, 
    NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>,
    Uuid, serde_json::Value, Vec<u8>
)]
impl TryFrom<Cell> for Option<Vec<Option<T>>> {
    type Error = &'static str;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(None),
            Cell::Array(ArrayCell::Null) => Ok(None),
            Cell::Array(array_cell) => {
                TryInto::<Vec<Option<T>>>::try_into(array_cell)
                    .map(Some)
                    .map_err(|_| "type conversion failed")
            }
            _ => Err("Only ArrayCell can be converted to Option<Vec<Option<${T}>>>"),
        }
    }
}

#[derive(Debug, Clone, TryInto)]
pub enum ArrayCell {
    #[try_into(ignore)]
    Null,
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    U32(Vec<Option<u32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Numeric(Vec<Option<PgNumeric>>),
    Date(Vec<Option<NaiveDate>>),
    Time(Vec<Option<NaiveTime>>),
    TimeStamp(Vec<Option<NaiveDateTime>>),
    TimeStampTz(Vec<Option<DateTime<Utc>>>),
    Uuid(Vec<Option<Uuid>>),
    Json(Vec<Option<serde_json::Value>>),
    Bytes(Vec<Option<Vec<u8>>>),
}