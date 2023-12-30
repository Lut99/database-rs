//  SQL.rs
//    by Lut99
//
//  Created:
//    27 Dec 2023, 11:33:39
//  Last edited:
//    30 Dec 2023, 12:11:57
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
use num_traits::AsPrimitive;


/***** SERIALIZATION *****/
/// Formats an [`ToSql`]-enabled type to some formatter.
pub struct ToSqlFormatter<'o, O> {
    /// The object to serialize.
    obj: &'o O,
}
impl<'o, O: ToSql> Display for ToSqlFormatter<'o, O> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult { self.obj.fmt_sql(f) }
}

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
impl ToSql for Statement {
    #[inline]
    fn fmt_sql(&self, f: &mut Formatter) -> FResult {
        match self {
            Self::CreateTable(ct) => ct.fmt_sql(f),
            Self::UseDatabase(ud) => ud.fmt_sql(f),
        }
    }
}
impl AsRef<Statement> for Statement {
    #[inline]
    fn as_ref(&self) -> &Self { self }
}
impl AsMut<Statement> for Statement {
    #[inline]
    fn as_mut(&mut self) -> &mut Self { self }
}
impl From<&Statement> for Statement {
    #[inline]
    fn from(value: &Self) -> Self { value.clone() }
}
impl From<&mut Statement> for Statement {
    #[inline]
    fn from(value: &mut Self) -> Self { value.clone() }
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
impl ToSql for StatementCreateTable {
    fn fmt_sql(&self, f: &mut Formatter) -> FResult {
        // Write the statement up to the columns
        write!(f, "CREATE TABLE \"{}\" (", self.name)?;

        // Serialize the columns
        let mut first: bool = true;
        for col in &self.cols {
            // Write the separator
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }

            // Write the column definition
            col.fmt_sql(f)?;
        }

        // Write the closing parenthesis, end
        write!(f, "}};")
    }
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

    /// Changes the name of this column.
    ///
    /// # Arguments
    /// - `name`: The name to change into.
    ///.
    /// # Returns
    /// Self for chaining.
    #[inline]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Changes the type of this column.
    ///
    /// # Arguments
    /// - `ty`: The type to change into.
    ///.
    /// # Returns
    /// Self for chaining.
    ///
    /// # Panics
    /// If a `default` is given (i.e., [`Some`]) and the new type of this column is not compatible with the default's type, then this function will panic.
    #[inline]
    pub fn ty(mut self, ty: impl Into<Type>) -> Self {
        let ty: Type = ty.into();
        if let Some(default) = &self.default {
            if !default.ty().compatible_with(&ty) {
                panic!(
                    "Cannot change type of column to {} when it already has a0 default value with type {}",
                    self.ty.variant(),
                    default.ty().variant(),
                );
            }
        }
        self.ty = ty;
        self
    }

    /// Changes whether this column will automatically increment for every new row.
    ///
    /// # Arguments
    /// - `auto_increment`: Whether to enable this option or not.
    ///
    /// # Returns
    /// Self for chaining.
    #[inline]
    pub fn auto_increment(mut self, auto_increment: impl AsPrimitive<bool>) -> Self {
        self.auto_increment = auto_increment.as_();
        self
    }

    /// Changes whether this column can have NULL in or not.
    ///
    /// # Arguments
    /// - `not_null`: Whether to enable this option or not. Note that this is _reversed_ (i.e., enter `false` to allow NULL).
    ///
    /// # Returns
    /// Self for chaining.
    #[inline]
    pub fn not_null(mut self, not_null: impl AsPrimitive<bool>) -> Self {
        self.not_null = not_null.as_();
        self
    }

    /// Changes whether new rows will be instantiated with a default value.
    ///
    /// # Arguments
    /// - `value`: If [`Some`], then the column will be instantiated with this value; else, enter [`None`].
    ///
    /// # Returns
    /// Self for chaining.
    ///
    /// # Panics
    /// If the type of the given value does not match the type of the column, this function will panic.
    #[inline]
    pub fn default(mut self, default: Option<Value>) -> Self {
        if let Some(default) = &default {
            if !default.ty().compatible_with(&self.ty) {
                panic!("Cannot give a value of type {} as default value for a column with type {}", default.ty().variant(), self.ty.variant());
            }
        }
        self.default = default;
        self
    }
}
impl ToSql for ColumnDef {
    #[inline]
    fn fmt_sql(&self, f: &mut Formatter) -> FResult {
        // Write the name
        write!(f, "\"{}\" ", self.name)?;
        // Write the type
        self.ty.fmt_sql(f)?;
        // Write any options
        if self.auto_increment {
            write!(f, " AUTO_INCREMENT")?;
        }
        if self.not_null {
            write!(f, " NOT NULL")?;
        }
        if let Some(default) = &self.default {
            write!(f, " DEFAULT ")?;
            default.fmt_sql(f)?;
        }

        // Ok!
        Ok(())
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
impl Type {
    /// Returns whether this Type is compatible with the given Type (i.e., they trivially cast to each other).
    ///
    /// # Arguments
    /// - `other`: The other type to compare compatability with.
    ///
    /// # Returns
    /// Whether this type casts into the other.
    pub fn compatible_with(&self, other: &Type) -> bool {
        use Type::*;
        match (self, other) {
            // Booleans are castable to any numeric type
            (Boolean, BigInt | BigIntUnsigned | Int | IntUnsigned | SmallInt | SmallIntUnsigned | TinyInt | TinyIntUnsigned | Float(_) | Real) => {
                true
            },

            // Smaller ints cast to larger ints
            (TinyInt, SmallInt | Int | BigInt)
            | (TinyIntUnsigned, SmallIntUnsigned | IntUnsigned | BigIntUnsigned)
            | (SmallInt, Int | BigInt)
            | (SmallIntUnsigned, IntUnsigned | BigIntUnsigned)
            | (Int, BigInt)
            | (IntUnsigned, BigIntUnsigned) => true,

            // Ints cast to reals
            (TinyInt | TinyIntUnsigned | SmallInt | SmallIntUnsigned | Int | IntUnsigned | BigInt | BigIntUnsigned, Float(_) | Real) => true,

            // Strings cast to each other if their size permits
            (Character(llen) | VarChar(llen) | Clob(llen), Character(rlen) | VarChar(rlen) | Clob(rlen)) => *llen <= *rlen,

            // Dates & Times upcast to DateTimes
            (Date | Time, DateTime) => true,

            // Otherwise, things that are equal to themselves are always compatible
            (this, other) => this == other,
        }
    }
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
