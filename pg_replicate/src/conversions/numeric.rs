// adapted from the bigdecimal crate
#[cfg(feature = "bigdecimal")]
use bigdecimal::{
    num_bigint::{BigInt, BigUint, Sign},
    BigDecimal, ParseBigDecimalError,
};
use byteorder::{BigEndian, ReadBytesExt};
use derive_more::TryInto;
#[cfg(feature = "rust_decimal")]
use rust_decimal::Decimal;
use std::{fmt::Display, io::Cursor, str::FromStr};
use tokio_postgres::types::{FromSql, Type};

/// A rust variant of the Postgres Numeric type. The full spectrum of Postgres'
/// Numeric value range is supported.
///
/// Represented as an Optional BigDecimal. None for 'NaN', Some(bigdecimal) for
/// all other values.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, TryInto)]
pub enum PgNumeric {
    NaN,
    PositiveInf,
    NegativeInf,
    #[cfg(feature = "bigdecimal")]
    Value(BigDecimal),
    #[cfg(feature = "rust_decimal")]
    Value(Decimal),
    #[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
    Value(String),
}

#[cfg(feature = "rust_decimal")]
#[derive(Debug, thiserror::Error)]
pub enum ParseDecimalError {
    #[error("parsing decimal: {0}")]
    RustDecimalError(rust_decimal::Error),

    #[error("invalid decimal value")]
    InvalidDecimalValue,
}

#[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
#[derive(Debug, thiserror::Error)]
pub struct ParseNumericInfallible;

#[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
impl std::fmt::Display for ParseNumericInfallible {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unreachable!()
    }
}

impl FromStr for PgNumeric {
    #[cfg(feature = "bigdecimal")]
    type Err = ParseBigDecimalError;

    #[cfg(feature = "rust_decimal")]
    type Err = ParseDecimalError;

    #[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
    type Err = ParseNumericInfallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "bigdecimal")]
        let conv = BigDecimal::from_str(s);
        #[cfg(feature = "rust_decimal")]
        let conv = Decimal::from_str(s).map_err(ParseDecimalError::RustDecimalError);
        #[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
        let conv = match s.to_lowercase().as_str() {
            "infinity" | "-infinity" | "nan" => Err(ParseNumericInfallible),
            _other => Ok(s.to_string()),
        };
        match conv {
            Ok(n) => Ok(PgNumeric::Value(n)),
            Err(e) => {
                if s.to_lowercase() == "infinity" {
                    Ok(PgNumeric::PositiveInf)
                } else if s.to_lowercase() == "-infinity" {
                    Ok(PgNumeric::NegativeInf)
                } else if s.to_lowercase() == "nan" {
                    Ok(PgNumeric::NaN)
                } else {
                    Err(e)
                }
            }
        }
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let mut rdr = Cursor::new(raw);

        let n_digits = rdr.read_u16::<BigEndian>()?;
        let weight = rdr.read_i16::<BigEndian>()?;
        #[derive(PartialEq, Eq)]
        enum PgSign {
            Minus,
            Plus,
        }
        let sign = match rdr.read_u16::<BigEndian>()? {
            0x4000 => PgSign::Minus,
            0x0000 => PgSign::Plus,
            0xC000 => return Ok(PgNumeric::NaN),
            0xD000 => return Ok(PgNumeric::PositiveInf),
            0xF000 => return Ok(PgNumeric::NegativeInf),
            v => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid sign {v:#04x}"),
                )
                .into())
            }
        };
        let scale = rdr.read_u16::<BigEndian>()?;

        #[cfg(feature = "bigdecimal")]
        let res = {
            let mut biguint = BigUint::from(0u32);
            for n in (0..n_digits).rev() {
                let digit = rdr.read_u16::<BigEndian>()?;
                biguint += BigUint::from(digit) * BigUint::from(10_000u32).pow(n as u32);
            }

            // First digit in unsigned now has factor 10_000^(digits.len() - 1),
            // but should have 10_000^weight
            //
            // Credits: this logic has been copied from rust Diesel's related code
            // that provides the same translation from Postgres numeric into their
            // related rust type.
            let correction_exp = 4 * (i64::from(weight) - i64::from(n_digits) + 1);
            let sign = match sign {
                PgSign::Minus => Sign::Minus,
                PgSign::Plus => Sign::Plus,
            };
            BigDecimal::new(BigInt::from_biguint(sign, biguint), -correction_exp)
                .with_scale(i64::from(scale))
        };

        #[cfg(feature = "rust_decimal")]
        let res = {
            let mut digits = Vec::with_capacity(n_digits as usize);
            for _ in 0..n_digits {
                digits.push(rdr.read_u16::<BigEndian>()?);
            }
            match checked_from_postgres(sign == PgSign::Minus, weight, scale, digits) {
                Some(res) => res,
                None => Err(ParseDecimalError::InvalidDecimalValue)?,
            }
        };

        #[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
        let res: String = {
            let _n_digits = n_digits;
            let _weight = weight;
            let _scale = scale;
            let _sign = sign;
            "TODO".to_string() // TODO: format the digits directly
        };

        Ok(PgNumeric::Value(res))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

impl Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgNumeric::NaN => write!(f, "NaN"),
            PgNumeric::PositiveInf => write!(f, "Infinity"),
            PgNumeric::NegativeInf => write!(f, "-Infinity"),
            PgNumeric::Value(n) => write!(f, "{n}"),
        }
    }
}

impl Default for PgNumeric {
    fn default() -> Self {
        #[cfg(feature = "bigdecimal")]
        let value = PgNumeric::Value(BigDecimal::default());
        #[cfg(feature = "rust_decimal")]
        let value = PgNumeric::Value(Decimal::default());
        #[cfg(not(any(feature = "bigdecimal", feature = "rust_decimal")))]
        let value = PgNumeric::Value("0".to_string());
        value
    }
}

#[cfg(feature = "rust_decimal")]
fn checked_from_postgres(
    neg: bool,
    weight: i16,
    scale: u16,
    mut digits: Vec<u16>,
) -> Option<Decimal> {
    // waiting for a rust_decimal > 1.36 to introduce this as Decimal::MAX_SCALE
    const MAX_SCALE: u32 = 28;

    // From https://github.com/paupino/rust-decimal/blob/46fb4c3c517bc0c27cd534b65f9e8b57c24ba18e/src/postgres/common.rs
    let fractionals_part_count = digits.len() as i32 + (-weight as i32) - 1;
    let integers_part_count = weight as i32 + 1;

    let mut result = Decimal::ZERO;
    // adding integer part
    if integers_part_count > 0 {
        let (start_integers, last) = if integers_part_count > digits.len() as i32 {
            (
                integers_part_count - digits.len() as i32,
                digits.len() as i32,
            )
        } else {
            (0, integers_part_count)
        };
        let integers: Vec<_> = digits.drain(..last as usize).collect();
        for digit in integers {
            result = result.checked_mul(Decimal::from_i128_with_scale(10i128.pow(4), 0))?;
            result = result.checked_add(Decimal::new(digit as i64, 0))?;
        }
        result = result.checked_mul(Decimal::from_i128_with_scale(
            10i128.pow(4 * start_integers as u32),
            0,
        ))?;
    }
    // adding fractional part
    if fractionals_part_count > 0 {
        let start_fractionals = if weight < 0 { (-weight as u32) - 1 } else { 0 };
        for (i, digit) in digits.into_iter().enumerate() {
            let fract_pow = 4_u32.checked_mul(i as u32 + 1 + start_fractionals)?;
            if fract_pow <= MAX_SCALE {
                result = result.checked_add(
                    Decimal::new(digit as i64, 0)
                        / Decimal::from_i128_with_scale(10i128.pow(fract_pow), 0),
                )?;
            } else if fract_pow == MAX_SCALE + 4 {
                // rounding last digit
                if digit >= 5000 {
                    result = result.checked_add(
                        Decimal::new(1_i64, 0)
                            / Decimal::from_i128_with_scale(10i128.pow(MAX_SCALE), 0),
                    )?;
                }
            }
        }
    }

    result.set_sign_negative(neg);
    // Rescale to the postgres value, automatically rounding as needed.
    result.rescale((scale as u32).min(MAX_SCALE));
    Some(result)
}
