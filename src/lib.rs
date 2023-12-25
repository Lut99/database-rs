//  LIB.rs
//    by Lut99
//
//  Created:
//    17 Dec 2023, 19:56:11
//  Last edited:
//    25 Dec 2023, 12:25:12
//  Auto updated?
//    Yes
//
//  Description:
//!   Provides various [`Database`]s that can be used as basis for
//!   use-case specific database connectors.
//

// Declare the various databases supported
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Declare other modules
pub mod common;
