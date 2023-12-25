//  SQLITE.rs
//    by Lut99
//
//  Created:
//    17 Dec 2023, 20:50:18
//  Last edited:
//    25 Dec 2023, 17:54:07
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


/***** ERRORS *****/
/// Defines errors originating in the SQLite [`Database`]
#[derive(Debug)]
pub enum Error<E> {
    /// Failed to load the config file.
    ConfigLoad { err: crate::common::Error },
    /// Failed to open the target database file.
    DatabaseOpen { path: PathBuf, err: sqlite::Error },
    /// The initialization code failed.
    InitFailed { path: PathBuf, err: E },
}
impl<E> Display for Error<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult {
        use Error::*;
        match self {
            ConfigLoad { .. } => write!(f, "Failed to load SQLite configuration file"),
            DatabaseOpen { path, .. } => write!(f, "Failed to open database file '{}'", path.display()),
            InitFailed { path, .. } => write!(f, "Failed to initialize SQLite database file '{}'", path.display()),
        }
    }
}
impl<E: 'static + error::Error> error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use Error::*;
        match self {
            ConfigLoad { err } => Some(err),
            DatabaseOpen { err, .. } => Some(err),
            InitFailed { err, .. } => Some(err),
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
    pub fn new<F, E>(path: impl AsRef<Path>, init: F) -> Result<Self, Error<E>>
    where
        F: FnOnce(&mut Self) -> Result<(), E>,
        E: 'static + error::Error,
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
        let mut this: Self = Self { conn };
        if run_init {
            debug!("Initializing database at '{}' with {}", path.display(), type_name::<F>());
            if let Err(err) = init(&mut this) {
                return Err(Error::InitFailed { path: path.into(), err });
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
    pub fn from_path<F, E>(cfg_path: impl AsRef<Path>, init: F) -> Result<Self, Error<E>>
    where
        F: FnOnce(&mut Self) -> Result<(), E>,
        E: 'static + error::Error,
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
}
