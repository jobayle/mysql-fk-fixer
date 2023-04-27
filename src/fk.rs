//! Foreign Key Constraint infos and query function

use std::fmt::Display;
use std::ops::Add;
use std::path::Path;
use std::{fs, io};
use std::io::{stdout, Write, ErrorKind, BufWriter};

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
    pub dump_invalid_rows: bool,
    pub dump_location: String,
}

impl FkChecker {

    /// Checks that dump_location exists and is writeable
    /// If not exist, will try to create the forlder
    pub fn new(auto_delete: bool, dump_invalid_rows: bool, dump_location: String) -> Result<Self> {
        if !dump_location.is_empty() {
            let path = Path::new(dump_location.as_str());
            if !path.exists() {
                fs::create_dir(path)?;
            }
            if !path.is_dir() {
                return Err(Error::IoError(io::Error::new(ErrorKind::Other, "Dump location is not a directory")));
            }
            let attr = fs::metadata(path)?;
            if attr.permissions().readonly() {
                return Err(Error::IoError(io::Error::new(ErrorKind::PermissionDenied, "Dump location is not writeable")));
            }
        }

        Ok(Self { auto_delete, dump_invalid_rows, dump_location })
    }

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

        if self.dump_invalid_rows && !ids.is_empty() {
            self.dump_rows(fk_info, &ids, conn)?;
        }

        if self.auto_delete && !ids.is_empty() {
            self.delete_all(fk_info, &ids, conn)?;
        }

        let res: Vec<T> = ids.into_iter() // into_iter consumes the collection
            .map(|id| T::from_value(id))
            .collect();

        return Ok(res)
    }

    /// Deletes all rows having an invalid foreign reference
    /// Performs one batch query
    fn delete_all(&self, fk_info: &FkInfo, ids: &Vec<Value>, conn: &mut Conn)-> Result<()>  {
        let query = format!("DELETE FROM {}.{} WHERE {}=?", fk_info.schema, fk_info.table, fk_info.column);
        query.with(ids.iter().map(|x| (x,))).batch(conn)
    }

    /// Dumps all rows
    fn dump_rows(&self, fk_info: &FkInfo, ids: &Vec<Value>, conn: &mut Conn) -> Result<()> {
        let query = format!("SELECT * FROM {}.{} WHERE {}=?", fk_info.schema, fk_info.table, fk_info.column);
        let preped = conn.prep(query)?;

        let mut col_disp = true;
        // Output to dump_location, or to stdout
        let mut out: Box<dyn io::Write> = match self.dump_location.is_empty() {
            true => Box::new(stdout()),
            false => {
                let fname = fk_info.name.clone().add(".csv");
                let path = Path::new(self.dump_location.as_str()).join(fname);
                Box::new(BufWriter::new(fs::File::create(path)?))
            }
        };

        for id in ids {
            let it = conn.exec_iter(&preped, (id,))?;

            if col_disp {
                for col in it.columns().as_ref() {
                    out.write(col.name_ref())?;
                    out.write(", ".as_bytes())?;
                }
                out.write("\n".as_bytes())?;
                col_disp = false;
            }
            
            for mut row in it.flat_map(|rs| rs.into_iter()) {

                for idx in 0..row.len() {
                    let val = row.take::<Value, usize>(idx)
                        .map(|v| v.as_sql(true))
                        .unwrap_or_else(|| String::from("NULL"));
                    out.write(val.as_bytes())?;
                    out.write(", ".as_bytes())?;
                }
                out.write("\n".as_bytes())?;
            }
        }
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use std::{fs::{File, self}, path::Path};

    use super::FkChecker;

    #[test]
    fn should_handle_empty_dump_location() {
        let foo = FkChecker::new(true, true, String::new());
        assert!(foo.is_ok());
        let bar = foo.unwrap();
        assert!(bar.auto_delete);
        assert!(bar.dump_invalid_rows);
        assert!(bar.dump_location.is_empty());
    }

    #[test]
    fn invalid_dump_location_is_err() {
        let path = Path::new("dump_file.txt");
        File::create(path).expect("create should work in test folder");
        assert!(path.exists());

        let dump_loc: String = String::from(path.to_str().unwrap());
        let foo = FkChecker::new(true, true, dump_loc);
        assert!(foo.is_err());
        fs::remove_file(path).expect("delete should work in test folder")
    }

    #[test]
    fn should_create_dump_folder() {
        let dump_loc_str = "dump_dir1";
        let path = Path::new(dump_loc_str);
        if path.exists() {
            fs::remove_dir(path).expect("dump_dir should be removable in test folder");
        }

        let dump_loc: String = String::from(dump_loc_str);
        let foo = FkChecker::new(false, false, dump_loc);
        assert!(foo.is_ok());
        let bar = foo.unwrap();
        assert!(!bar.auto_delete);
        assert!(!bar.dump_invalid_rows);
        assert_eq!(bar.dump_location, dump_loc_str);
        assert!(path.is_dir());
        fs::remove_dir(path).expect("dump_dir should be removable in test folder");
    }

    #[test]
    fn should_use_dump_folder() {
        let dump_loc_str = "dump_dir2";
        let path = Path::new(dump_loc_str);
        if !path.exists() {
            fs::create_dir(path).expect("dump_dir should be creatable in test folder");
        }

        let dump_loc: String = String::from(dump_loc_str);
        let foo = FkChecker::new(false, false, dump_loc);
        assert!(foo.is_ok());
        let bar = foo.unwrap();
        assert!(!bar.auto_delete);
        assert!(!bar.dump_invalid_rows);
        assert_eq!(bar.dump_location, dump_loc_str);
        assert!(path.is_dir());
        fs::remove_dir(path).expect("dump_dir should be removable in test folder");
    }
}
