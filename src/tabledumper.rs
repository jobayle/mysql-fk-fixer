use std::path::Path;
use std::fs::File;
use std::io::BufWriter;

use mysql::Conn;
use mysql::Result;
use mysql::Value;
use mysql::prelude::*;

use crate::datadumper;

pub fn dump_table(conn: &mut Conn, schema: &String, table: &String) -> Result<()> {
    let fname: String = format!("{schema}_{table}.csv");
    let path = Path::new("dumps").join(fname);
    let mut out = BufWriter::new(File::create(path)?);

    let mut disp_cols = true;

    let query = format!("SELECT * FROM {}.{} LIMIT 1,10;", schema, table);
    let it = conn.query_iter(query)?;
    for row in it {
        let mut row = row?;
        if disp_cols {
            datadumper::dump_columns(&mut out, row.columns_ref())?;
            disp_cols = false;
        }
        datadumper::dump_row(&mut out, &mut row)?;
    }
    Ok(())
}

pub fn dump_all_tables(conn: &mut Conn, schema: &String) -> Result<()> {
    let it = conn.query_map("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='flow';", |t: Value| String::from_value(t))?
        .into_iter();
    for table in it {
        dump_table(conn, schema, &table)?;
    }
    Ok(())
}
