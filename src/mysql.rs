//  MYSQL.rs
//    by Lut99
//
//  Created:
//    17 Dec 2023, 18:33:54
//  Last edited:
//    17 Dec 2023, 19:59:04
//  Auto updated?
//    Yes
//
//  Description:
//!   Implements [`Database`] for a MySQL backend.
//

use std::error;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter, Result as FResult};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use enum_debug::EnumDebug;
use log::{debug, info};
use mysql::{Opts, OptsBuilder, Pool};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};


/***** DEFAULTS *****/
/// Determines the port used for MySQL when the user specifies none.
const fn default_port() -> u16 { 3306 }





/***** ERRORS *****/
/// Defines errors originating in the MySQL [`Database`].
#[derive(Debug)]
pub enum Error {
    /// Failed to open a given file.
    FileOpen { path: PathBuf, err: std::io::Error },
    /// Failed to read the given file as a [`ConfigFile`].
    FileRead { kind: &'static str, path: PathBuf, err: Box<dyn error::Error> },
    /// Failed to create a new ConnectionPool.
    PoolCreate { opts: Opts, err: mysql::Error },
    /// Unknown extension for given config file path.
    UnknownExt { path: PathBuf },
}
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult {
        use Error::*;
        match self {
            FileOpen { path, .. } => write!(f, "Failed to open file '{}'", path.display()),
            FileRead { kind, path, .. } => write!(f, "Failed to read file '{}' as a {} credentials file", path.display(), kind),
            PoolCreate { opts, .. } => write!(
                f,
                "Failed to create new MySQL connection pool to 'mysql://{}:{}{}'",
                opts.get_ip_or_hostname(),
                opts.get_tcp_port(),
                if let Some(db_name) = opts.get_db_name() { format!("/{db_name}") } else { String::new() },
            ),
            UnknownExt { path } => write!(f, "Unknown extension for credentials file '{}' (expected 'json', 'yml' or 'yaml')", path.display()),
        }
    }
}
impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use Error::*;
        match self {
            FileOpen { err, .. } => Some(err),
            FileRead { err, .. } => Some(&**err),
            PoolCreate { err, .. } => Some(err),
            UnknownExt { .. } => None,
        }
    }
}





/***** HELPERS *****/
/// Defines a file with the MySQL config such that we know how to connect to the database.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConfigFile {
    /// The hostname of the server to connect to.
    host:     String,
    /// The port of the server to connect to.
    #[serde(default = "default_port")]
    port:     u16,
    /// The name of the database to connect to.
    #[serde(alias = "db", alias = "db_name", alias = "db-name")]
    database: String,
    /// The credentials used to connect to the server.
    creds:    Credentials,
}

/// Defines [`serde`]-compatible credentials.
#[derive(Clone, Debug, Deserialize, EnumDebug, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Credentials {
    /// It's a username/password pair.
    UsernamePassword(UsernamePassword),
}
impl AsRef<Credentials> for Credentials {
    #[inline]
    fn as_ref(&self) -> &Credentials { self }
}
impl AsMut<Credentials> for Credentials {
    #[inline]
    fn as_mut(&mut self) -> &mut Credentials { self }
}
impl From<&Credentials> for Credentials {
    #[inline]
    fn from(value: &Credentials) -> Self { value.clone() }
}
impl From<&mut Credentials> for Credentials {
    #[inline]
    fn from(value: &mut Credentials) -> Self { value.clone() }
}

/// Defines [`serde`]-compatible username/password pair credentials.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UsernamePassword {
    /// The name of the user.
    #[serde(alias = "name", alias = "user")]
    username: String,
    /// The password of the user.
    #[serde(alias = "pass")]
    password: String,
}





/***** LIBRARY *****/
/// Implementation of a [`spec::Database`] for a MySQL backend.
pub struct Database {
    /// The MySQL connection pool we use to connect to the MySQL database.
    pool: Pool,
}
impl Database {
    /// Constructor for the Database that initializes it pointing to a particular database.
    ///
    /// # Arguments
    /// - `hostname`: The hostname of the MySQL endpoint to connect to.
    /// - `port`: The port of the MySQL endpoint to connect to.
    /// - `database`: The specific database to connect with.
    /// - `creds`: A [`Credentials`] that describes how to authenticate ourselves to the server.
    ///
    /// # Returns
    /// A new instance of Self that can be used to communicate to a backend database.
    ///
    /// # Errors
    /// This function may error if we failed to connect to the given endpoint.
    #[inline]
    pub fn new(
        hostname: impl AsRef<str>,
        port: impl AsPrimitive<u16>,
        database: impl AsRef<str>,
        creds: impl AsRef<Credentials>,
    ) -> Result<Self, Error> {
        let hostname: &str = hostname.as_ref();
        let port: u16 = port.as_();
        let database: &str = database.as_ref();
        let creds: &Credentials = creds.as_ref();
        info!("Initializing MySQL database to '{hostname}:{port}'");

        // Prepare the options
        debug!("Preparing connection options...");
        let mut opts: OptsBuilder = OptsBuilder::new().ip_or_hostname(Some(hostname)).tcp_port(port).db_name(Some(database));
        match creds {
            Credentials::UsernamePassword(up) => {
                opts = opts.user(Some(&up.username)).pass(Some(&up.password));
            },
        }

        // Create the connection pool itself
        debug!("Creating MySQL connection pool...");
        let pool: Pool = match Pool::new(opts.clone()) {
            Ok(pool) => pool,
            Err(err) => return Err(Error::PoolCreate { opts: opts.into(), err }),
        };

        // OK, return ourselves
        Ok(Self { pool })
    }

    /// Constructor for the Database that initializes it pointing to a particular database.
    ///
    /// # Arguments
    /// - `cfg_path`: The path to the [`ConfigFile`] that we'll be reading.
    ///
    /// # Returns
    /// A new instance of Self that can be used to communicate to a backend database.
    ///
    /// # Errors
    /// This function may error if we failed to read the given file or if we failed to connect to the given endpoint.
    pub fn from_path(cfg_path: impl AsRef<Path>) -> Result<Self, Error> {
        let cfg_path: &Path = cfg_path.as_ref();
        info!("Initializing MySQL database by reading the options from '{}'", cfg_path.display());

        // Attempt to read the credentials file
        debug!("Loading config file '{}'...", cfg_path.display());
        let config: ConfigFile = match File::open(cfg_path) {
            Ok(mut handle) => {
                if cfg_path.extension().map(|ext| ext == OsStr::new("json")).unwrap_or(false) {
                    debug!("Config file '{}' is JSON", cfg_path.display());
                    match serde_json::from_reader(handle) {
                        Ok(config) => config,
                        Err(err) => return Err(Error::FileRead { kind: "JSON", path: cfg_path.into(), err: Box::new(err) }),
                    }
                } else if cfg_path.extension().map(|ext| ext == OsStr::new("yml") || ext == OsStr::new("yaml")).unwrap_or(false) {
                    debug!("Config file '{}' is YAML", cfg_path.display());
                    match serde_yaml::from_reader(handle) {
                        Ok(creds) => creds,
                        Err(err) => return Err(Error::FileRead { kind: "YAML", path: cfg_path.into(), err: Box::new(err) }),
                    }
                } else if cfg_path.extension().map(|ext| ext == OsStr::new("toml")).unwrap_or(false) {
                    debug!("Config file '{}' is TOML", cfg_path.display());

                    // Read it in its entirety first
                    let mut raw: String = String::new();
                    if let Err(err) = handle.read_to_string(&mut raw) {
                        return Err(Error::FileRead { kind: "UTF-8", path: cfg_path.into(), err: Box::new(err) });
                    }

                    // Parse as TOML
                    match toml::from_str(&raw) {
                        Ok(creds) => creds,
                        Err(err) => return Err(Error::FileRead { kind: "TOML", path: cfg_path.into(), err: Box::new(err) }),
                    }
                } else {
                    return Err(Error::UnknownExt { path: cfg_path.into() });
                }
            },
            Err(err) => return Err(Error::FileOpen { path: cfg_path.into(), err }),
        };

        // Now call the normal initializer with these options
        Self::new(config.host, config.port, config.database, config.creds)
    }
}
