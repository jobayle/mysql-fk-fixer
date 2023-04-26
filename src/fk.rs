//! Foreign Key Constraint infos and query function

use std::fmt::Display;

use mysql::*;
use mysql::prelude::*;

/// All the needed info to check a Foreign Key
#[allow(dead_code)]
#[derive(Debug)]
pub struct FkInfo {
    pub name: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
}

impl FkInfo {
    /// Create a new FkInfo from a Row
    pub fn new(row: (Value, Value, Value, Value, Value, Value)) -> Self {
        FkInfo{
            name: String::from_value(row.0),
            schema: String::from_value(row.1),
            table: String::from_value(row.2),
            column: String::from_value(row.3),
            ref_table: String::from_value(row.4),
            ref_column: String::from_value(row.5),
        }
    }

    /// Get all the FK constraints using given connection to MySQL (should be using schema information_schema)
    pub fn query_fk_constraints(conn: &mut Conn) -> Result<Vec<Self>> {
        let res = conn.query_map(
            r"SELECT
                k.CONSTRAINT_NAME,
                k.CONSTRAINT_SCHEMA,
                k.TABLE_NAME,
                k.COLUMN_NAME,
                k.REFERENCED_TABLE_NAME,
                k.REFERENCED_COLUMN_NAME
            FROM KEY_COLUMN_USAGE k
            JOIN TABLE_CONSTRAINTS c ON k.CONSTRAINT_NAME=c.CONSTRAINT_NAME AND c.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA
            WHERE c.CONSTRAINT_TYPE='FOREIGN KEY';", |t| FkInfo::new(t))?;
    
        Ok(res)
    }
}

impl Display for FkInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} in schema {} on table {} column {} references table {} column {}",
            self.name, self.schema, self.table, self.column, self.ref_table, self.ref_column)
    }
}

pub struct FkChecker {}

impl FkChecker {
    pub fn check(fk_info: &FkInfo, conn: &mut Conn) -> Result<Vec<u32>> {
        let query = format!(
            r"SELECT a.{}
            FROM {} a
            LEFT JOIN {} b ON a.{}=b.{}
            WHERE a.{} IS NOT NULL AND b.{} IS NULL;",
            fk_info.column, fk_info.table, fk_info.ref_table, fk_info.column, fk_info.ref_column, fk_info.column, fk_info.ref_column);

        return Ok(conn.query_map(query, |id: u32| id)?)
    }
}
