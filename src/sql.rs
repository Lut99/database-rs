//  SQL.rs
//    by Lut99
//
//  Created:
//    27 Dec 2023, 11:33:39
//  Last edited:
//    27 Dec 2023, 12:28:59
//  Auto updated?
//    Yes
//
//  Description:
//!   Implements an AST(-like) tree for SQL queries to make them
//!   type-safe and all that.
//

use std::fmt::{Display, Formatter, Result as FResult};

use chrono::{DateTime, Utc};
use enum_debug::EnumDebug;


/***** SERIALIZATION *****/
/// Implemented for all nodes in the SQL AST.
pub trait ToSql {
    /// Formats this node to an SQL string.
    ///
    /// # Arguments
    /// - `f`: The [`Formatter`] to which we write.
    ///
    /// # Errors
    /// This function may fail if we failed to write to the formatter.
    fn fmt_sql(&self, f: &mut Formatter) -> FResult;
}

/// Formats an [`ToSql`]-enabled type to some formatter.
pub struct ToSqlFormatter<'o, O> {
    /// The object to serialize.
    obj: &'o O,
}
impl<'o, O: ToSql> Display for ToSqlFormatter<'o, O> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult { self.obj.fmt_sql(f) }
}

/// Allows a given [`ToSql`]-enabled type to be serialized to a formatter.
///
/// # Arguments
/// - `obj`: The [`ToSql`]-like type that will be serialized.
///
/// # Returns
/// A [`ToSqlFormatter`] that implements [`Display`] and that does the actual formatting.
///
/// # Example
/// ```rust
/// use database::sql::{serialize_sql, StatementUseDatabase};
///
/// let stmt = StatementUseDatabase { name: "foo".into() };
/// assert_eq!(serialize_sql(&stmt).to_string(), "USE foo;");
/// ```
#[inline]
pub fn serialize_sql<O: ToSql>(obj: &O) -> ToSqlFormatter<O> { ToSqlFormatter { obj } }





/***** LIBRARY *****/
/// Toplevel thing: the SQL-statement
#[derive(Clone, Debug, EnumDebug)]
pub enum Statement {
    /// Creates a new table in the currently selected database.
    ///
    /// ```sql
    /// CREATE TABLE foo (bar UNSIGNED INT, baz VARCHAR(32));
    /// ```
    CreateTable(StatementCreateTable),

    /// Tells the database to use a different database.
    ///
    /// ```sql
    /// USE foo;
    /// ```
    UseDatabase(StatementUseDatabase),
}



/// Statement for creating tables.
///
/// ```sql
/// CREATE TABLE foo (bar UNSIGNED INT, baz VARCHAR(32));
/// ```
#[derive(Clone, Debug)]
pub struct StatementCreateTable {
    /// The name of the table to create.
    pub name: String,
    /// The definitions for each column in the table.
    pub cols: Vec<ColumnDef>,
}

/// Describes how to define a column in statements like [`StatementCreateTable`].
#[derive(Clone, Debug)]
pub struct ColumnDef {
    /// The name of the column.
    pub name: String,
    /// The datatype of the column.
    pub ty:   Type,

    /// Whether this column auto-increments.
    pub auto_increment: bool,
    /// Whether this column can be NULL, but negated (i.e., `false` means it can be NULL, `true` means it cannot).
    pub not_null: bool,
    /// Whether new rows will have this column initialized to a default value or not. If so, then this fields denotes that value.
    pub default: Option<Value>,
}
impl ColumnDef {
    /// Creates a new ColumnDef from a name and type only.
    ///
    /// All optional things (e.g., `AUTO_INCREMENT`) are not used. Use the builder methods for that.
    ///
    /// # Arguments
    /// - `name`: The name of the column.
    /// - `ty`: The data type of  the column, as a [`Type`].
    ///
    /// # Returns
    /// A new [`ColumnDef`].
    #[inline]
    pub fn new(name: impl Into<String>, ty: impl Into<Type>) -> Self {
        Self {
            name: name.into(),
            ty:   ty.into(),

            auto_increment: false,
            not_null: false,
            default: None,
        }
    }
}



/// Statement for switching active databases.
///
/// ```sql
/// USE foo;
/// ```
#[derive(Clone, Debug)]
pub struct StatementUseDatabase {
    /// The name of the database to switch to.
    pub name: String,
}
impl ToSql for StatementUseDatabase {
    #[inline]
    fn fmt_sql(&self, f: &mut Formatter) -> FResult { write!(f, "USE {};", self.name) }
}



/// Enumerates possible data types in SQL.
#[derive(Clone, Copy, Debug, EnumDebug, Eq, Hash, PartialEq)]
pub enum Type {
    // Numeric types
    /// A boolean (akin to `bool`).
    Boolean,
    /// Signed 64-bit integer (akin to `i64`).
    BigInt,
    /// Unsigned 64-bit integer (akin to `u64`).
    BigIntUnsigned,
    /// Signed 32-bit integer (akin to `i32`).
    Int,
    /// Unsigned 32-bit integer (akin to `u32`).
    IntUnsigned,
    /// Signed 16-bit integer (akin to `i16`).
    SmallInt,
    /// Unigned 16-bit integer (akin to `u16`).
    SmallIntUnsigned,
    /// Signed 8-bit integer (akin to `i8`).
    TinyInt,
    /// Unigned 8-bit integer (akin to `u8`).
    TinyIntUnsigned,
    /// A floating-point value with a given precision (in number of bits).
    Float(usize),
    /// A 64-bit floating-point value.
    Real,

    // String types
    /// A fixed-length array of characters. Smaller strings are padded with blank characters.
    Character(usize),
    /// A fixed-length array of characters, except that it also stores strings smaller than its size.
    VarChar(usize),

    // Date/time types
    /// Defines a date-only time store.
    Date,
    /// Defines a time-only time store.
    Time,
    /// Combines date and time values into one time store.
    DateTime,

    // Miscellaneous
    /// Defines a Binary Large Object of the given number of bytes that is larger than a single allowed column width thingy.
    Blob(usize),
    /// Defines a Character Large Object of the given number of characters that is larger than a single allowed column width thingy.
    Clob(usize),
}
impl ToSql for Type {
    fn fmt_sql(&self, f: &mut Formatter) -> FResult {
        use Type::*;
        match self {
            Boolean => write!(f, "BIT"),
            BigInt => write!(f, "BIGINT"),
            BigIntUnsigned => write!(f, "BIGINT UNSIGNED"),
            Int => write!(f, "INT"),
            IntUnsigned => write!(f, "INT UNSIGNED"),
            SmallInt => write!(f, "SMALLINT"),
            SmallIntUnsigned => write!(f, "SMALLINT UNSIGNED"),
            TinyInt => write!(f, "TINYINT"),
            TinyIntUnsigned => write!(f, "TINYINT UNSIGNED"),
            Float(size) => write!(f, "FLOAT({size})"),
            Real => write!(f, "REAL"),

            Character(len) => write!(f, "CHARACTER({len})"),
            VarChar(len) => write!(f, "VARCHAR({len})"),

            Date => write!(f, "DATE"),
            Time => write!(f, "TIME"),
            DateTime => write!(f, "TIMESTAMP"),

            Blob(size) => write!(f, "BLOB({size})"),
            Clob(len) => write!(f, "BLOB({len})"),
        }
    }
}

/// Enumerates possible values in SQL.
#[derive(Clone, Debug, EnumDebug)]
pub enum Value {
    // Numeric values
    /// A boolean value.
    Boolean(bool),
    /// Signed 64-bit integer value.
    BigInt(i64),
    /// Unsigned 64-bit integer value.
    BigIntUnsigned(u64),
    /// Signed 32-bit integer value.
    Int(i32),
    /// Unsigned 32-bit integer value.
    IntUnsigned(u32),
    /// Signed 16-bit integer value.
    SmallInt(i16),
    /// Unigned 16-bit integer value.
    SmallIntUnsigned(u16),
    /// Signed 8-bit integer value.
    TinyInt(i8),
    /// Unigned 8-bit integer value.
    TinyIntUnsigned(u8),
    /// A 32-bit floating-point value.
    Float(f32),
    /// A 64-bit floating-point value.
    Double(f64),

    // String values
    /// String value.
    String(String),

    // Datetime values
    /// Date and/or time value.
    DateTime(DateTime<Utc>),

    // Miscellaneous values
    /// A large binary object wrapped in a `Blob`.
    Blob(Vec<u8>),
    /// A large character object wrapped in a `Clob`.
    Clob(String),
}
impl Value {
    /// Returns the datatype of this value.
    ///
    /// # Returns
    /// A [`Type`] describing this value.
    pub fn ty(&self) -> Type {
        use Value::*;
        match self {
            Boolean(_) => Type::Boolean,
            BigInt(_) => Type::BigInt,
            BigIntUnsigned(_) => Type::BigIntUnsigned,
            Int(_) => Type::Int,
            IntUnsigned(_) => Type::IntUnsigned,
            SmallInt(_) => Type::SmallInt,
            SmallIntUnsigned(_) => Type::SmallIntUnsigned,
            TinyInt(_) => Type::TinyInt,
            TinyIntUnsigned(_) => Type::TinyIntUnsigned,
            Float(_) => Type::Float(32),
            Double(_) => Type::Float(64),

            String(s) => Type::Character(s.len()),

            DateTime(_) => Type::DateTime,

            Blob(b) => Type::Blob(b.len()),
            Clob(c) => Type::Clob(c.len()),
        }
    }
}
impl ToSql for Value {
    fn fmt_sql(&self, fmt: &mut Formatter) -> FResult {
        use Value::*;
        match self {
            Boolean(b) => write!(fmt, "{}", if *b { '1' } else { '0' }),
            BigInt(b) => write!(fmt, "{b}"),
            BigIntUnsigned(b) => write!(fmt, "{b}"),
            Int(i) => write!(fmt, "{i}"),
            IntUnsigned(i) => write!(fmt, "{i}"),
            SmallInt(s) => write!(fmt, "{s}"),
            SmallIntUnsigned(s) => write!(fmt, "{s}"),
            TinyInt(t) => write!(fmt, "{t}"),
            TinyIntUnsigned(t) => write!(fmt, "{t}"),
            Float(f) => write!(fmt, "{f}"),
            Double(d) => write!(fmt, "{d}"),

            String(s) => write!(fmt, "\"{s}\""),

            DateTime(dt) => write!(fmt, "{}", dt.format("%Y-%m-%d %H:%M:%S")),

            Blob(_) => todo!(),
            Clob(c) => write!(fmt, "\"{c}\""),
        }
    }
}
