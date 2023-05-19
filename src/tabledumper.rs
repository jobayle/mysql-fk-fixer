use std::collections::HashSet;
use std::path::Path;
use std::fs::File;
use std::io::BufWriter;

use mysql::{Column, Result, Row, Value};
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

pub fn dump_table<T>(conn: &mut T, schema: &String, table: &String) -> Result<()>
    where T: Queryable
{
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

pub fn dump_all_tables<T>(conn: &mut T, schema: &String) -> Result<()>
    where T: Queryable
{
    let query = format!("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='{schema}';");
    let it = conn.query_map(query, |t: Value| String::from_value(t))?
        .into_iter();
    for table in it {
        dump_table(conn, schema, &table)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use mysql_common::constants::ColumnType;
    use super::*;

    #[test]
    fn test_check_nulls() {
        let mut null_cols = HashSet::from([String::from("col1"), String::from("col2")]);

        let columns = Arc::new([
            Column::new(ColumnType::MYSQL_TYPE_BIT).with_name("col1".as_bytes()), // Still null
            Column::new(ColumnType::MYSQL_TYPE_BIT).with_name("col2".as_bytes()), // Not null anymore
            Column::new(ColumnType::MYSQL_TYPE_BIT).with_name("col3".as_bytes()), // Not in Set
        ]);
        let row = mysql_common::row::new_row(vec![Value::NULL, Value::Int(22), Value::Int(0)], columns);

        check_nulls(&mut null_cols, &row);

        assert!(null_cols.len() == 1);
        assert!(null_cols.contains(&String::from("col1")));
    }
}
