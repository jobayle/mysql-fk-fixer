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
            FROM information_schema.KEY_COLUMN_USAGE k
            JOIN information_schema.TABLE_CONSTRAINTS c ON k.CONSTRAINT_NAME=c.CONSTRAINT_NAME AND c.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA
            WHERE c.CONSTRAINT_TYPE='FOREIGN KEY';", |t| FkInfo::new(t))?;
    
        Ok(res)
    }
}

impl Display for FkInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} in schema {} on table {} column {} referencing table {} column {}",
            self.name, self.schema, self.table, self.column, self.ref_table, self.ref_column)
    }
}

/// Configuration for function check()
pub struct FkChecker {
    pub auto_delete: bool,
}

impl FkChecker {
    /// Return a list of all invalid foreign references.
    /// Deletes the invalid rows if auto_delete is true.
    /// Param T should be the type of the foreign column.
    pub fn check<T>(&self, fk_info: &FkInfo, conn: &mut Conn) -> Result<Vec<T>>
    where T: FromValue
    {
        let query = format!(
            r"SELECT a.{}
            FROM {}.{} a
            LEFT JOIN {}.{} b ON a.{}=b.{}
            WHERE a.{} IS NOT NULL AND b.{} IS NULL;",
            fk_info.column, fk_info.schema, fk_info.table, fk_info.schema, fk_info.ref_table, fk_info.column, fk_info.ref_column, fk_info.column, fk_info.ref_column);

        let ids = conn.query::<Value, String>(query)?;
        if self.auto_delete {
            self.delete_all(fk_info, &ids, conn);
        }

        let res: Vec<T> = ids.into_iter() // into_iter consumes the collection
            .map(|id| T::from_value(id))
            .collect();

        return Ok(res)
    }

    fn delete_all(&self, fk_info: &FkInfo, ids: &Vec<Value>, conn: &mut Conn) {
        let query = format!("DELETE FROM {}.{} WHERE {}=?", fk_info.schema, fk_info.table, fk_info.column);
        let bar = query.with(ids.iter().map(|x| (x,))).batch(conn);
        if let Err(error) = bar {
            println!("ERROR: Could not batch delete rows having an invalid foreign reference from table {}.{}", fk_info.schema, fk_info.table);
            println!("ERROR: {error}");
        }
    }
}
