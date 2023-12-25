//  COMMON.rs
//    by Lut99
//
//  Created:
//    25 Dec 2023, 12:25:23
//  Last edited:
//    25 Dec 2023, 12:31:10
//  Auto updated?
//    Yes
//
//  Description:
//!   Defines some things used across databases.
//

use std::error;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter, Result as FResult};
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use log::debug;
use serde::de::DeserializeOwned;


/***** ERRORS *****/
/// Defines errors originate from the common part of the `database` crate.
#[derive(Debug)]
pub enum Error {
    /// Failed to open a given file.
    FileOpen { path: PathBuf, err: std::io::Error },
    /// Failed to read the given file as a [`ConfigFile`].
    FileRead { kind: &'static str, path: PathBuf, err: Box<dyn error::Error> },
    /// Unknown extension for given config file path.
    UnknownExt { path: PathBuf },
}
impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FResult {
        use Error::*;
        match self {
            FileOpen { path, .. } => write!(f, "Failed to open file '{}'", path.display()),
            FileRead { kind, path, .. } => write!(f, "Failed to read file '{}' as a {} credentials file", path.display(), kind),
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
            UnknownExt { .. } => None,
        }
    }
}





/***** LIBRARY FUNCTIONS *****/
/// Loads a [`Deserialize`]able type from the given path, using various backends depending on the given path's extension.
///
/// # Generics
/// - `F`: The type to load to.
///
/// # Arguments
/// - `path`: The path to load the file from.
///
/// # Returns
/// A new instance of `F` loaded from disk.
///
/// # Errors
/// This function may error if an I/O error occurred.
pub fn load_config_file<F: DeserializeOwned>(path: impl AsRef<Path>) -> Result<F, Error> {
    let path: &Path = path.as_ref();

    // Attempt to read the credentials file
    debug!("Loading config file '{}'...", path.display());
    let config: F = match File::open(path) {
        Ok(mut handle) => {
            if path.extension().map(|ext| ext == OsStr::new("json")).unwrap_or(false) {
                debug!("Config file '{}' is JSON", path.display());
                match serde_json::from_reader(handle) {
                    Ok(config) => config,
                    Err(err) => return Err(Error::FileRead { kind: "JSON", path: path.into(), err: Box::new(err) }),
                }
            } else if path.extension().map(|ext| ext == OsStr::new("yml") || ext == OsStr::new("yaml")).unwrap_or(false) {
                debug!("Config file '{}' is YAML", path.display());
                match serde_yaml::from_reader(handle) {
                    Ok(creds) => creds,
                    Err(err) => return Err(Error::FileRead { kind: "YAML", path: path.into(), err: Box::new(err) }),
                }
            } else if path.extension().map(|ext| ext == OsStr::new("toml")).unwrap_or(false) {
                debug!("Config file '{}' is TOML", path.display());

                // Read it in its entirety first
                let mut raw: String = String::new();
                if let Err(err) = handle.read_to_string(&mut raw) {
                    return Err(Error::FileRead { kind: "UTF-8", path: path.into(), err: Box::new(err) });
                }

                // Parse as TOML
                match toml::from_str(&raw) {
                    Ok(creds) => creds,
                    Err(err) => return Err(Error::FileRead { kind: "TOML", path: path.into(), err: Box::new(err) }),
                }
            } else {
                return Err(Error::UnknownExt { path: path.into() });
            }
        },
        Err(err) => return Err(Error::FileOpen { path: path.into(), err }),
    };

    // Dope done
    Ok(config)
}
