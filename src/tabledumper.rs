//! Table data dumper, CSV format

use std::io;

use mysql::*;
use mysql::consts::ColumnType;

pub fn dump_columns(out: &mut dyn io::Write, columns: &[Column]) -> Result<()> {
    for col in columns {
        out.write(col.name_ref())?;
        out.write(", ".as_bytes())?;
    }
    out.write("\n".as_bytes())?;
    for col in columns {
        out.write(coltype_to_str(col.column_type()).as_bytes())?;
        out.write(", ".as_bytes())?;
    }
    out.write("\n".as_bytes())?;
    Ok(())
}

pub fn dump_row(out: &mut dyn io::Write, row: &mut Row) -> Result<()> {
    for idx in 0..row.len() {
        let val = value_to_string(row.take::<Value, usize>(idx), &row.columns_ref()[idx]);
        out.write(val.as_bytes())?;
        out.write(", ".as_bytes())?;
    }
    out.write("\n".as_bytes())?;
    Ok(())
}

/// Cast value to String according to the data/column-type/column-length
pub fn value_to_string(val: Option<Value>, column: &Column) -> String {
    let coltype = column.column_type();
    let mut res: Option<String> = None;
    if val.is_some() {
        // Case boolean
        if coltype == ColumnType::MYSQL_TYPE_BIT && column.column_length() == 1 {
            res = val
                .filter (|v| if let Value::Bytes(_) = v { true } else { false })
                .map(|v| if let Value::Bytes(vec) = v { vec } else { unreachable!() })
                .map(|v| if v.len() == 1 && v[0] == 1 { "true" } else { "false" })
                .map(String::from);
        }
        // Case binary data
        else if coltype == ColumnType::MYSQL_TYPE_BIT
            || coltype == ColumnType::MYSQL_TYPE_LONG_BLOB
            || coltype == ColumnType::MYSQL_TYPE_TINY_BLOB
            || coltype == ColumnType::MYSQL_TYPE_MEDIUM_BLOB {
            res = Some( String::from("<Binary data>"));
        }
        else {
            res = val.map(|v| v.as_sql(true));
        }
    }
    res.unwrap_or_else(|| String::from("NULL"))
}

/// Return a printable column type
pub fn coltype_to_str(coltype: ColumnType) -> &'static str {
    match coltype {
        ColumnType::MYSQL_TYPE_DECIMAL => "DECIMAL",
        ColumnType::MYSQL_TYPE_TINY => "TINY",
        ColumnType::MYSQL_TYPE_SHORT => "SHORT",
        ColumnType::MYSQL_TYPE_LONG => "LONG",
        ColumnType::MYSQL_TYPE_FLOAT => "FLOAT",
        ColumnType::MYSQL_TYPE_DOUBLE => "DOUBLE",
        ColumnType::MYSQL_TYPE_NULL => "NULL",
        ColumnType::MYSQL_TYPE_TIMESTAMP => "TIMESTAMP",
        ColumnType::MYSQL_TYPE_LONGLONG => "LONGLONG",
        ColumnType::MYSQL_TYPE_INT24 => "INT24",
        ColumnType::MYSQL_TYPE_DATE => "DATE",
        ColumnType::MYSQL_TYPE_TIME => "TIME",
        ColumnType::MYSQL_TYPE_DATETIME => "DATETIME",
        ColumnType::MYSQL_TYPE_YEAR => "YEAR",
        ColumnType::MYSQL_TYPE_NEWDATE => "NEWDATE",
        ColumnType::MYSQL_TYPE_VARCHAR => "VARCHAR",
        ColumnType::MYSQL_TYPE_BIT => "BIT",
        ColumnType::MYSQL_TYPE_TIMESTAMP2 => "TIMESTAMP2",
        ColumnType::MYSQL_TYPE_DATETIME2 => "DATETIME2",
        ColumnType::MYSQL_TYPE_TIME2 => "TIME2",
        ColumnType::MYSQL_TYPE_TYPED_ARRAY => "TYPED_ARRAY",
        ColumnType::MYSQL_TYPE_UNKNOWN => "UNKNOWN",
        ColumnType::MYSQL_TYPE_JSON => "JSON",
        ColumnType::MYSQL_TYPE_NEWDECIMAL => "NEWDECIMAL",
        ColumnType::MYSQL_TYPE_ENUM => "ENUM",
        ColumnType::MYSQL_TYPE_SET => "SET",
        ColumnType::MYSQL_TYPE_TINY_BLOB => "TINY_BLOB",
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => "MEDIUM_BLOB",
        ColumnType::MYSQL_TYPE_LONG_BLOB => "LONG_BLOB",
        ColumnType::MYSQL_TYPE_BLOB => "BLOB",
        ColumnType::MYSQL_TYPE_VAR_STRING => "VAR_STRING",
        ColumnType::MYSQL_TYPE_STRING => "STRING",
        ColumnType::MYSQL_TYPE_GEOMETRY => "GEOMETRY",
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use std::{io::Cursor, sync::Arc};

    #[test]
    fn test_dump_columns() {
        let mut write = Cursor::new(vec![0u8; 128]);

        let mut check = |coltype| {
            let columns = [Column::new(coltype).with_name(b"name")];
            write.set_position(0);
            assert!(dump_columns(&mut write, &columns).is_ok()); // No IO error

            let end: usize = write.position().try_into().unwrap();
            let res = String::from_utf8_lossy(&write.get_mut().as_slice()[0..end]);
            let cmp = format!("name, \n{}, \n", coltype_to_str(coltype));
            assert_eq!(res, cmp);
        };

        for it in [
            ColumnType::MYSQL_TYPE_DECIMAL,
            ColumnType::MYSQL_TYPE_TINY,
            ColumnType::MYSQL_TYPE_SHORT,
            ColumnType::MYSQL_TYPE_LONG,
            ColumnType::MYSQL_TYPE_FLOAT,
            ColumnType::MYSQL_TYPE_DOUBLE,
            ColumnType::MYSQL_TYPE_NULL,
            ColumnType::MYSQL_TYPE_TIMESTAMP,
            ColumnType::MYSQL_TYPE_LONGLONG,
            ColumnType::MYSQL_TYPE_INT24,
            ColumnType::MYSQL_TYPE_DATE,
            ColumnType::MYSQL_TYPE_TIME,
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnType::MYSQL_TYPE_YEAR,
            ColumnType::MYSQL_TYPE_NEWDATE,
            ColumnType::MYSQL_TYPE_VARCHAR,
            ColumnType::MYSQL_TYPE_BIT,
            ColumnType::MYSQL_TYPE_TIMESTAMP2,
            ColumnType::MYSQL_TYPE_DATETIME2,
            ColumnType::MYSQL_TYPE_TIME2,
            ColumnType::MYSQL_TYPE_TYPED_ARRAY,
            ColumnType::MYSQL_TYPE_UNKNOWN,
            ColumnType::MYSQL_TYPE_JSON,
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
            ColumnType::MYSQL_TYPE_ENUM,
            ColumnType::MYSQL_TYPE_SET,
            ColumnType::MYSQL_TYPE_TINY_BLOB,
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            ColumnType::MYSQL_TYPE_LONG_BLOB,
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_GEOMETRY
        ] {
            check(it);
        }
    }
    
    #[test]
    fn test_dump_row() {
        let columns = Arc::new([Column::new(ColumnType::MYSQL_TYPE_BIT)]);
        let mut row = mysql_common::row::new_row(vec![Value::NULL], columns);

        let mut write = Cursor::new(vec![0u8; 128]);

        assert!(dump_row(&mut write, &mut row).is_ok()); // No IO error

        let end: usize = write.position().try_into().unwrap();
        let res = String::from_utf8_lossy(&write.get_mut().as_slice()[0..end]);
        assert_eq!(res, "<Binary data>, \n");
    }

    #[test]
    fn test_value_to_string() {
        let column = Column::new(ColumnType::MYSQL_TYPE_VARCHAR);
        let res = value_to_string(Option::Some(Value::Bytes(Vec::<u8>::from("value"))), &column);
        assert_eq!(res, "'value'");
    }
    
    #[test]
    fn test_value_to_string_none() {
        let column = Column::new(ColumnType::MYSQL_TYPE_BIT);
        let res = value_to_string(Option::None, &column);
        assert_eq!(res, "NULL");
    }

    #[test]
    fn test_value_to_string_boolean() {
        let column = Column::new(ColumnType::MYSQL_TYPE_BIT);
        let column = column.with_column_length(1);
        let res = value_to_string(Option::Some(Value::Bytes(vec![1])), &column);
        assert_eq!(res, "true");
        let res = value_to_string(Option::Some(Value::Bytes(vec![0])), &column);
        assert_eq!(res, "false");
    }

    #[test]
    fn test_value_to_string_binary_data() {
        let check = |coltype: ColumnType| {
            let column = Column::new(coltype);
            let res = value_to_string(Option::Some(Value::NULL), &column);
            assert_eq!(res, "<Binary data>");
        };
        check(ColumnType::MYSQL_TYPE_BIT);
        check(ColumnType::MYSQL_TYPE_LONG_BLOB);
        check(ColumnType::MYSQL_TYPE_TINY_BLOB);
        check(ColumnType::MYSQL_TYPE_MEDIUM_BLOB);
    }

    #[test]
    fn test_coltype_to_str() {
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_DECIMAL), "DECIMAL");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TINY), "TINY");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_SHORT), "SHORT");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_LONG), "LONG");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_FLOAT), "FLOAT");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_DOUBLE), "DOUBLE");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_NULL), "NULL");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TIMESTAMP), "TIMESTAMP");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_LONGLONG), "LONGLONG");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_INT24), "INT24");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_DATE), "DATE");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TIME), "TIME");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_DATETIME), "DATETIME");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_YEAR), "YEAR");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_NEWDATE), "NEWDATE");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_VARCHAR), "VARCHAR");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_BIT), "BIT");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TIMESTAMP2), "TIMESTAMP2");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_DATETIME2), "DATETIME2");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TIME2), "TIME2");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TYPED_ARRAY), "TYPED_ARRAY");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_UNKNOWN), "UNKNOWN");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_JSON), "JSON");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_NEWDECIMAL), "NEWDECIMAL");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_ENUM), "ENUM");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_SET), "SET");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_TINY_BLOB), "TINY_BLOB");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_MEDIUM_BLOB), "MEDIUM_BLOB");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_LONG_BLOB), "LONG_BLOB");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_BLOB), "BLOB");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_VAR_STRING), "VAR_STRING");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_STRING), "STRING");
        assert_eq!(coltype_to_str(ColumnType::MYSQL_TYPE_GEOMETRY), "GEOMETRY");
    }
}
