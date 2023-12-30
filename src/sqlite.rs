//  SQLITE.rs
//    by Lut99
//
//  Created:
//    17 Dec 2023, 20:50:18
//  Last edited:
//    30 Dec 2023, 12:57:44
//  Auto updated?
//    Yes
//
//  Description:
//!   Implements [`Database`] for an SQLite backend.
//

use std::any::type_name;
use std::error;
use std::fmt::{Display, Formatter, Result as FResult};
use std::path::{Path, PathBuf};

use log::{debug, info};
use serde::{Deserialize, Serialize};
pub use sqlite as backend;
use sqlite::Connection;

use crate::common::load_config_file;
use crate::sql::{serialize_sql, Statement};


/***** ERRORS *****/
/// Defines errors originating in the SQLite [`Database`]
#[derive(Debug)]
pub enum Error {
    /// Failed to load the config file.
    ConfigLoad { err: crate::common::Error },
    /// Failed to open the target database file.
    DatabaseOpen { path: PathBuf, err: sqlite::Error },
    /// The initialization code failed.
    InitFailed { path: PathBuf, err: Box<Self> },

    /// Failed to execute the given query.
    ExecuteFailed { query: String, err: sqlite::Error },
}
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult {
        use Error::*;
        match self {
            ConfigLoad { .. } => write!(f, "Failed to load SQLite configuration file"),
            DatabaseOpen { path, .. } => write!(f, "Failed to open database file '{}'", path.display()),
            InitFailed { path, .. } => write!(f, "Failed to initialize SQLite database file '{}'", path.display()),

            ExecuteFailed { query, .. } => write!(f, "Failed to execute statement '{query}'"),
        }
    }
}
impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use Error::*;
        match self {
            ConfigLoad { err } => Some(err),
            DatabaseOpen { err, .. } => Some(err),
            InitFailed { err, .. } => Some(&**err),

            ExecuteFailed { err, .. } => Some(err),
        }
    }
}





/***** HELPERS *****/
/// Defines a file with the SQLite config such that we know how to connect to the database.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConfigFile {
    /// The path to the database file.
    path: PathBuf,
}





/***** LIBRARY *****/
/// Implementation of a [`spec::Database`] for an SQLite backend.
pub struct Database {
    /// The connection to the database.
    conn: Connection,
}
impl Database {
    /// Constructor for the Database that initializes it pointing to a particular database.
    ///
    /// # Arguments
    /// - `path`: The path to the database file.
    /// - `init`: If the database did not previously exist, this code may be executed to initialize it.
    ///
    /// # Returns
    /// A new instance of Self that can be used to communicate to a backend database.
    ///
    /// # Errors
    /// This function may error if we failed to connect to the given endpoint.
    #[inline]
    pub fn new<F>(path: impl AsRef<Path>, init: F) -> Result<Self, Error>
    where
        F: FnOnce(&Self) -> Result<(), Error>,
    {
        let path: &Path = path.as_ref();
        info!("Initializing SQLite database at '{}'", path.display());

        // See if the database exists or not
        let run_init: bool = !path.exists();

        // Attempt to open the connection
        debug!("Opening connection to '{}'...", path.display());
        let conn: Connection = match sqlite::open(path) {
            Ok(conn) => conn,
            Err(err) => return Err(Error::DatabaseOpen { path: path.into(), err }),
        };

        // Run the init, if necessary
        let this: Self = Self { conn };
        if run_init {
            debug!("Initializing database at '{}' with {}", path.display(), type_name::<F>());
            if let Err(err) = init(&this) {
                return Err(Error::InitFailed { path: path.into(), err: Box::new(err) });
            }
        }

        // OK, return ourselves
        Ok(this)
    }

    /// Constructor for the Database that initializes it pointing to a particular database.
    ///
    /// # Arguments
    /// - `cfg_path`: The path to the [`ConfigFile`] that we'll be reading.
    /// - `init`: If the database did not previously exist, this code may be executed to initialize it.
    ///
    /// # Returns
    /// A new instance of Self that can be used to communicate to a backend database.
    ///
    /// # Errors
    /// This function may error if we failed to read the given file or if we failed to connect to the given endpoint.
    pub fn from_path<F>(cfg_path: impl AsRef<Path>, init: F) -> Result<Self, Error>
    where
        F: FnOnce(&Self) -> Result<(), Error>,
    {
        let cfg_path: &Path = cfg_path.as_ref();
        info!("Initializing SQLite database by reading the options from '{}'", cfg_path.display());

        // Defer to the common part
        match load_config_file::<ConfigFile>(cfg_path) {
            Ok(config) => {
                // Now call the normal initializer with these options
                Self::new(config.path, init)
            },
            Err(err) => Err(Error::ConfigLoad { err }),
        }
    }

    /// Executes the given SQL [`Statement`] on the backend.
    ///
    /// Note that the query is serialized as-is. To use a prepared statement, see `Self::execute_prepared()`.
    ///
    /// Any results of the query are discarded. See `Self::query()` to send a statement and return the rows.
    ///
    /// # Arguments
    /// - `stmt`: The [`Statement`] to execute.
    ///
    /// # Errors
    /// This function errors if we failed to execute the given `stmt` for some reason.
    pub fn execute(&self, stmt: impl AsRef<Statement>) -> Result<(), Error> {
        let stmt: &Statement = stmt.as_ref();

        // Serialize directly and send
        let query: String = serialize_sql(stmt).to_string();
        match self.conn.execute(&query) {
            Ok(_) => Ok(()),
            Err(err) => Err(Error::ExecuteFailed { query, err }),
        }
    }
}
