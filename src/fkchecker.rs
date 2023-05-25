//! Foreign Key Constraint checker (finds invalid foreign references in the database)

use std::ops::Add;
use std::path::PathBuf;
use std::{fs, io};
use std::io::{stdout, ErrorKind, BufWriter};

use mysql::*;
use mysql::prelude::*;

use crate::fk::{FkInfo, FkIndex};
use crate::datadumper;

/// Configuration for function check()
pub struct FkChecker {
    pub auto_delete: bool,
    pub dump_invalid_rows: bool,
    pub dump_location: Option<PathBuf>,
}

impl FkChecker {

    /// Checks that dump_location exists and is writeable
    /// If not exist, will try to create the forlder
    pub fn new(auto_delete: bool, dump_invalid_rows: bool, dump_location: Option<PathBuf>) -> Result<Self> {
        if let Some(dump_loc) = &dump_location {
            if !dump_loc.exists() {
                fs::create_dir(dump_loc)?;
            }
            if !dump_loc.is_dir() {
                return Err(Error::IoError(io::Error::new(ErrorKind::Other, "Dump location is not a directory")));
            }
            let attr = fs::metadata(dump_loc)?;
            if attr.permissions().readonly() {
                return Err(Error::IoError(io::Error::new(ErrorKind::PermissionDenied, "Dump location is not writeable")));
            }
        }

        Ok(Self { auto_delete, dump_invalid_rows, dump_location })
    }

    /// Return a list of all invalid foreign references.
    /// Deletes the invalid rows if auto_delete is true.
    /// Param T should be the type of the foreign column.
    pub fn check<T, C>(&self, fk_info: &FkInfo, fk_idx: &FkIndex, conn: &mut C) -> Result<Vec<T>>
        where T: FromValue, C: Queryable
    {
        let query = format!(
            r"SELECT a.{}
            FROM {}.{} a
            LEFT JOIN {}.{} b ON a.{}=b.{}
            WHERE a.{} IS NOT NULL AND b.{} IS NULL;",
            fk_info.column, fk_info.schema, fk_info.table, fk_info.schema, fk_info.ref_table, fk_info.column, fk_info.ref_column, fk_info.column, fk_info.ref_column);

        let ids = conn.query::<Value, String>(query)?;

        if self.dump_invalid_rows && !ids.is_empty() {
            self.dump_rows(fk_info, &ids, fk_idx, conn)?;
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
    fn delete_all<C>(&self, fk_info: &FkInfo, ids: &Vec<Value>, conn: &mut C)-> Result<()>
        where C: Queryable
    {
        let query = format!("DELETE FROM {}.{} WHERE {}=?", fk_info.schema, fk_info.table, fk_info.column);
        conn.exec_batch(query, ids.iter().map(|x| (x,)))
    }

    /// Dumps all rows
    fn dump_rows<C>(&self, fk_info: &FkInfo, ids: &Vec<Value>, fk_idx: &FkIndex, conn: &mut C) -> Result<()>
        where C: Queryable
    {
        let mut query = format!("SELECT * FROM {}.{}", fk_info.schema, fk_info.table);
        if let Some(fks) = fk_idx.fks_by_table.get(&fk_info.table) {
            if fks.len() > 1 {
                if let Some(clause) = fks.iter()
                    .filter(|f| f.name != fk_info.name)
                    .map(|f| format!(" LEFT JOIN {}.{} ON {}.{}={}.{}", f.schema, f.ref_table, f.table, f.column, f.ref_table, f.ref_column))
                    .reduce(|acc, e| acc + &e) {
                        query = query.add(&clause);
                    }
            }
        }
        query = query + &format!(" WHERE {}=?;", fk_info.column);
        let preped = conn.prep(query)?;

        let mut col_disp = true;
        // Output to dump_location, or to stdout
        let mut out: Box<dyn io::Write> = match &self.dump_location {
            None => Box::new(stdout()),
            Some(dump_loc) => {
                let fname = fk_info.name.clone().add(".csv");
                let path = dump_loc.clone().join(fname);
                Box::new(BufWriter::new(fs::File::create(path)?))
            }
        };

        for id in ids {
            let it = conn.exec_iter(&preped, (id,))?;

            if col_disp {
                datadumper::dump_columns(out.as_mut(), it.columns().as_ref())?;
                col_disp = false;
            }
            
            for mut row in it.flat_map(|rs| rs.into_iter()) {
                datadumper::dump_row(out.as_mut(), &mut row)?
            }
            
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{fs::{File, self}, path::PathBuf};

    use super::FkChecker;

    #[test]
    fn should_handle_empty_dump_location() {
        let foo = FkChecker::new(true, true, None);
        assert!(foo.is_ok());
        let bar = foo.unwrap();
        assert!(bar.auto_delete);
        assert!(bar.dump_invalid_rows);
        assert!(bar.dump_location.is_none());
    }

    #[test]
    fn invalid_dump_location_is_err() {
        let dump_location = PathBuf::from("dump_file.txt");
        let path = dump_location.clone();
        File::create(&path).expect("create should work in test folder");
        assert!(path.exists());

        let foo = FkChecker::new(true, true, Some(dump_location));
        assert!(foo.is_err());
        fs::remove_file(path).expect("delete should work in test folder")
    }

    #[test]
    fn should_create_dump_folder() {
        let dump_loc = PathBuf::from("dump_dir1");
        let path = dump_loc.clone();
        if path.exists() {
            fs::remove_dir(&path).expect("dump_dir should be removable in test folder");
        }

        let foo = FkChecker::new(false, false, Some(dump_loc));
        assert!(foo.is_ok());
        let bar = foo.unwrap();
        assert!(!bar.auto_delete);
        assert!(!bar.dump_invalid_rows);
        assert_eq!(bar.dump_location.expect("dump_location should be Some"), path);
        assert!(path.is_dir());
        fs::remove_dir(path).expect("dump_dir should be removable in test folder");
    }

    #[test]
    fn should_use_dump_folder() {
        let dump_loc = PathBuf::from("dump_dir2");
        let path = dump_loc.clone();
        if !path.exists() {
            fs::create_dir(&path).expect("dump_dir should be creatable in test folder");
        }

        let foo = FkChecker::new(false, false, Some(dump_loc));
        assert!(foo.is_ok());
        let bar = foo.unwrap();
        assert!(!bar.auto_delete);
        assert!(!bar.dump_invalid_rows);
        assert_eq!(bar.dump_location.expect("dump_location should be Some"), path);
        assert!(path.is_dir());
        fs::remove_dir(path).expect("dump_dir should be removable in test folder");
    }
}
