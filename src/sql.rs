//  SQL.rs
//    by Lut99
//
//  Created:
//    25 Dec 2023, 18:13:29
//  Last edited:
//    26 Dec 2023, 21:29:08
//  Auto updated?
//    Yes
//
//  Description:
//!   Defines common implementations for all SQL-derived databases.
//

use std::fmt::{Display, Formatter, Result as FResult};
use std::str::FromStr;

use enum_debug::EnumDebug;


/***** ERRORS *****/
pub mod errors {
    use std::error::Error;
    use std::fmt::{Display, Formatter, Result as FResult};

    /// Failed to parse a [`ColumnType`] from a string.
    #[derive(Debug)]
    pub enum ColumnTypeParseError {
        /// We didn't recognize the type identifier given.
        UnknownIdentifier { raw: String },
    }
    impl Display for ColumnTypeParseError {
        #[inline]
        fn fmt(&self, f: &mut Formatter<'_>) -> FResult {
            use ColumnTypeParseError::*;
            match self {
                UnknownIdentifier { raw } => write!(f, "Unknown SQL column type '{raw}'"),
            }
        }
    }
    impl Error for ColumnTypeParseError {}
}





/***** FORMATTERS *****/
/// Formats a type implementing [`SqlColumnType`] using [`SqlColumnType::column_type_fmt()`].
pub struct SqlColumnTypeFormatter<'t, T: ?Sized> {
    /// The thing to format.
    obj: &'t T,
}
impl<'t, T: SqlColumnType> Display for SqlColumnTypeFormatter<'t, T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult { self.obj.column_type_fmt(f) }
}

/// Formats a type implementing [`SqlColumnDef`] using [`SqlColumnDef::column_def_fmt()`].
pub struct SqlColumnDefFormatter<'c, T: ?Sized> {
    /// The thing to format.
    obj: &'c T,
}
impl<'c, T: SqlColumnDef> Display for SqlColumnDefFormatter<'c, T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult { self.obj.column_def_fmt(f) }
}





/***** TRAITS *****/
/// Abstracts over possible ways to describe column types.
pub trait SqlColumnType {
    // Provided
    /// Formats this type as an SQL statement.
    ///
    /// # Arguments
    /// - `f`: A [`Formatter`] to write to.
    ///
    /// # Errors
    /// This function should error if it failed to write to the given formatter.
    fn column_type_fmt(&self, f: &mut Formatter) -> FResult;

    // Derived
    /// Serializes it to an SQL column definition.
    ///
    /// # Returns
    /// A [`ColumnTypeFormatter`] that implements [`Display`].
    #[inline]
    fn to_sql(&self) -> SqlColumnTypeFormatter<Self> { SqlColumnTypeFormatter { obj: self } }
}

/// Abstracts over possible ways to give the definition of a column.
pub trait SqlColumnDef {
    // Provided
    /// Formats this type as an SQL statement.
    ///
    /// # Arguments
    /// - `f`: A [`Formatter`] to write to.
    ///
    /// # Errors
    /// This function should error if it failed to write to the given formatter.
    fn column_def_fmt(&self, f: &mut Formatter) -> FResult;

    // Derived
    /// Serializes it to an SQL column definition.
    ///
    /// # Returns
    /// A [`ColumnDefFormatter`] that implements [`Display`].
    #[inline]
    fn to_sql(&self) -> SqlColumnDefFormatter<Self> { SqlColumnDefFormatter { obj: self } }
}
impl<K: Display, V: Display> SqlColumnDef for (K, V) {
    #[inline]
    fn column_def_fmt(&self, f: &mut Formatter) -> FResult { write!(f, "{} {}", self.0, self.1) }
}





/***** TYPES *****/
/// Lists all possible column types.
#[derive(Clone, Copy, Debug, EnumDebug, Eq, Hash, PartialEq)]
pub enum ColumnType {
    /// A one-byte, whole number.
    TinyInteger,
    /// A non-negative, whole number.
    UnsignedInteger,
}
impl SqlColumnType for ColumnType {
    #[inline]
    fn column_type_fmt(&self, f: &mut Formatter<'_>) -> FResult {
        use ColumnType::*;
        match self {
            TinyInteger => write!(f, "TINYINT"),
            UnsignedInteger => write!(f, "INT UNSIGNED"),
        }
    }
}
impl FromStr for ColumnType {
    type Err = errors::ColumnTypeParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tinyint" => Ok(Self::TinyInteger),
            "int unsigned" => Ok(Self::UnsignedInteger),
            _ => Err(errors::ColumnTypeParseError::UnknownIdentifier { raw: s.into() }),
        }
    }
}
impl AsRef<ColumnType> for ColumnType {
    #[inline]
    fn as_ref(&self) -> &Self { self }
}
impl AsMut<ColumnType> for ColumnType {
    #[inline]
    fn as_mut(&mut self) -> &mut Self { self }
}
impl From<&ColumnType> for ColumnType {
    #[inline]
    fn from(value: &Self) -> Self { *value }
}
impl From<&mut ColumnType> for ColumnType {
    #[inline]
    fn from(value: &mut Self) -> Self { *value }
}



// SqlColumnDef
/// Builder for a proper column definition query.
#[derive(Clone, Copy)]
pub struct ColumnDef<S> {
    /// The name of the column.
    pub name: S,
    /// The type of the column.
    pub ty: ColumnType,
    /// Whether the column is allowed to have `NULL` (false) or not (true).
    pub not_null: bool,
    /// Whether the column automatically increments.
    pub auto_increment: bool,
}
impl<S> ColumnDef<S> {
    /// Creates a new [`ColumnDef`] from a name and a datatype.
    ///
    /// # Arguments
    /// - `name`: The name of the new column definition.
    /// - `ty`: The type (as a [`ColumnType`]) of the column.
    ///
    /// # Returns
    /// A new [`ColumnDef`] that can be turned into a query.
    pub fn new(name: S, ty: impl Into<ColumnType>) -> Self { Self { name, ty: ty.into(), not_null: false, auto_increment: false } }

    /// Sets the name to something else.
    ///
    /// # Arguments
    /// - `name`: The name to set instead of the one given in the constructor.
    ///
    /// # Returns
    /// `self` for chaining.
    #[inline]
    pub fn set_name(&mut self, name: impl Into<S>) -> &mut Self {
        self.name = name.into();
        self
    }

    /// Sets the data type to something else.
    ///
    /// # Arguments
    /// - `ty`: The data type to set instead of the one given in the constructor.
    ///
    /// # Returns
    /// `self` for chaining.
    #[inline]
    pub fn set_ty(&mut self, ty: impl Into<ColumnType>) -> &mut Self {
        self.ty = ty.into();
        self
    }

    /// Sets whether the column cannot have any `NULL`-values (true) or whether it can (false).
    ///
    /// # Arguments
    /// - `not_null`: The value for this option.
    ///
    /// # Returns
    /// `self` for chaining.
    #[inline]
    pub fn set_not_null(&mut self, not_null: bool) -> &mut Self {
        self.not_null = not_null;
        self
    }

    /// Sets whether the column automagically increment on every new row.
    ///
    /// # Arguments
    /// - `auto_increment`: The value for this option.
    ///
    /// # Returns
    /// `self` for chaining.
    #[inline]
    pub fn set_auto_increment(&mut self, auto_increment: bool) -> &mut Self {
        self.auto_increment = auto_increment;
        self
    }
}
impl<S: Display> SqlColumnDef for ColumnDef<S> {
    #[inline]
    fn column_def_fmt(&self, f: &mut Formatter) -> FResult {
        // Write the name & type always
        write!(f, "{} {}", self.name, self.ty.to_sql())?;

        // Add extra options
        if self.not_null {
            write!(f, " NOT NULL")?;
        }
        if self.auto_increment {
            write!(f, " AUTO_INCREMENT")?;
        }

        // Alrighty done
        Ok(())
    }
}
