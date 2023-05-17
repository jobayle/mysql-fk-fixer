use std::collections::HashSet;
use std::path::Path;
use std::fs::File;
use std::io::BufWriter;

use mysql::{Column, Conn, Result, Row, Value};
use mysql::prelude::*;

use crate::datadumper;

fn check_nulls(null_cols: &mut HashSet<String>, row: &Row) {
    for i in 0..row.len()-1 {
        let keep = match row.get::<Value, usize>(i) {
            Some(cell) => matches!(cell, Value::NULL),
            None => true
        };
        if !keep {
            null_cols.remove(&row.columns_ref()[i].name_str().to_string());
        }
    }
}

pub fn dump_table(conn: &mut Conn, schema: &String, table: &String) -> Result<()> {
    let fname: String = format!("{schema}_{table}.csv");
    let path = Path::new("dumps").join(fname);
    let mut out = BufWriter::new(File::create(path)?);

    let mut disp_cols = true;
    let mut null_cols: HashSet<String> = HashSet::new();

    let query = format!("SELECT * FROM {schema}.{table} LIMIT 1,10;");
    let it = conn.query_iter(query)?;
    for row in it {
        let mut row = row?;
        if disp_cols {
            datadumper::dump_columns(&mut out, row.columns_ref())?;
            disp_cols = false;
            row.columns_ref().iter().map(Column::name_str).for_each(|n| { null_cols.insert(n.to_string()); });
        }
        check_nulls(&mut null_cols, &row);
        datadumper::dump_row(&mut out, &mut row)?;
    }
    for cn in null_cols.iter() {
        let query = format!("SELECT * FROM {schema}.{table} WHERE {cn} IS NOT NULL LIMIT 1;");
        for row in conn.query_iter(query)? {
            datadumper::dump_row(&mut out, &mut row?)?;
        }
    }
    Ok(())
}

pub fn dump_all_tables(conn: &mut Conn, schema: &String) -> Result<()> {
    let query = format!("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='{schema}';");
    let it = conn.query_map(query, |t: Value| String::from_value(t))?
        .into_iter();
    for table in it {
        dump_table(conn, schema, &table)?;
    }
    Ok(())
}
