use actix_web::{web, Error as AWError};
use rusqlite::{Statement, NO_PARAMS};
use serde::{Deserialize, Serialize};
use std::{thread::sleep, time::Duration};

pub type Pool = r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>;
pub type Connection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;

#[allow(clippy::enum_variant_names)]
pub enum Queries {
}