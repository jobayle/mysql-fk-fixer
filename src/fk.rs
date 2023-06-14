//! Foreign Key Constraint infos and query function

use std::collections::HashMap;
use std::fmt::Display;
use std::rc::Rc;

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
    pub fn query_fk_constraints<T>(conn: &mut T, schema: Option<&String>) -> Result<Vec<Self>>
        where T: Queryable
    {
        let mut query = String::from(
            r"SELECT
                k.CONSTRAINT_NAME,
                k.CONSTRAINT_SCHEMA,
                k.TABLE_NAME,
                k.COLUMN_NAME,
                k.REFERENCED_TABLE_NAME,
                k.REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE k
            JOIN information_schema.TABLE_CONSTRAINTS c ON k.CONSTRAINT_NAME=c.CONSTRAINT_NAME AND c.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA
            WHERE c.CONSTRAINT_TYPE='FOREIGN KEY'");
        if let Some(schema_name) = schema {
            query = format!("{query} AND k.CONSTRAINT_SCHEMA='{schema_name}'");
        }
        let res = conn.query_map(query, |t| FkInfo::new(t))?;
    
        Ok(res)
    }
}

impl Display for FkInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} in schema {} on table {} column {} referencing table {} column {}",
            self.name, self.schema, self.table, self.column, self.ref_table, self.ref_column)
    }
}

pub struct FkIndex {
    pub fks: Vec<Rc<FkInfo>>,
    pub fks_by_name: HashMap<String, Rc<FkInfo>>,
    pub fks_by_table: HashMap<String, Vec<Rc<FkInfo>>>,
    pub fks_by_ref_table: HashMap<String, Vec<Rc<FkInfo>>>,
}

// Pre indexed list of FkInfo, by constraint name / table name / referenced table name
impl From<Vec<FkInfo>> for FkIndex {
    fn from(value: Vec<FkInfo>) -> Self {
        let fks: Vec<Rc<FkInfo>> = value.into_iter().map(Rc::new).collect::<Vec<Rc<FkInfo>>>();
        let mut res = FkIndex { fks: fks, fks_by_name: HashMap::new(), fks_by_table: HashMap::new(), fks_by_ref_table: HashMap::new() };
        res.fks.iter().for_each(|v| assert!(res.fks_by_name.insert(v.name.clone(), v.clone()).is_none()));
        res.fks.iter().for_each(|v| res.fks_by_table.entry(v.table.clone()).or_default().push(v.clone()));
        res.fks.iter().for_each(|v| res.fks_by_ref_table.entry(v.ref_table.clone()).or_default().push(v.clone()));
        res
    }
}


#[cfg(test)]
mod test {
    use super::{FkIndex, FkInfo};
    use mysql::Value;

    #[test]
    fn test_from() {
        let fks = vec![
            FkInfo::new(
                (
                    Value::from("fk1"), // name
                    Value::from("sch"), // schema
                    Value::from("tb1"), // table
                    Value::from("col"), // column
                    Value::from("rt1"), // ref_table
                    Value::from("col")  // ref_column
                )
            ),
            FkInfo::new(
                (
                    Value::from("fk2"), // name
                    Value::from("sch"), // schema
                    Value::from("tb2"), // table
                    Value::from("col"), // column
                    Value::from("rt2"), // ref_table
                    Value::from("col")  // ref_column
                )
            )
        ];

        let index = FkIndex::from(fks);
        
        // Check FK1 successfully indexed
        let fk1 = index.fks_by_name.get("fk1");
        assert!(fk1.is_some());
        assert_eq!(fk1.unwrap().name, "fk1");
        
        let fks = index.fks_by_table.get("tb1");
        assert!(fks.is_some());
        assert_eq!(fks.unwrap().len(), 1);
        assert_eq!(fks.unwrap()[0].name, "fk1");
        
        let fks = index.fks_by_ref_table.get("rt1");
        assert!(fks.is_some());
        assert_eq!(fks.unwrap().len(), 1);
        assert_eq!(fks.unwrap()[0].name, "fk1");
        
        // Check FK2 successfully indexed
        let fk2 = index.fks_by_name.get("fk2");
        assert!(fk2.is_some());
        assert_eq!(fk2.unwrap().name, "fk2");
        
        let fks = index.fks_by_table.get("tb2");
        assert!(fks.is_some());
        assert_eq!(fks.unwrap().len(), 1);
        assert_eq!(fks.unwrap()[0].name, "fk2");
        
        let fks = index.fks_by_ref_table.get("rt2");
        assert!(fks.is_some());
        assert_eq!(fks.unwrap().len(), 1);
        assert_eq!(fks.unwrap()[0].name, "fk2");

    }

    #[test]
    fn test_fmt() {
        let fk = FkInfo {
            name: String::from("NAME"),
            schema: String::from("SCHEMA"),
            table: String::from("TABLE"),
            column: String::from("COLUMN"),
            ref_table: String::from("REF_TABLE"),
            ref_column: String::from("REF_COLUMN")
        };
        let res = format!("{}", fk);
        assert_eq!(res, "NAME in schema SCHEMA on table TABLE column COLUMN referencing table REF_TABLE column REF_COLUMN");
    }

}
