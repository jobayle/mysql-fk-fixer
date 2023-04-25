use std::fmt::Debug;

use sqlx::{mysql::{MySqlPoolOptions, MySqlRow}, MySqlConnection, Connection, error::BoxDynError, Executor};
use dotenvy_macro::dotenv;

#[derive(Debug)]
struct FkInfo {
    name: String,
    schema: String,
    table: String,
    column: String,
    ref_table: String,
    ref_column: String,
}

impl From<&MySqlRow> for FkInfo {
    fn from(row: &MySqlRow) -> Self {
        FkInfo{
            name: String::from(""),
            schema: String::from(""),
            table: String::from(""),
            column: String::from(""),
            ref_table: String::from(""),
            ref_column: String::from(""),
        }
    }
}

async fn query_fk_constraints(conn: &mut MySqlConnection) -> Result<Vec<FkInfo>, BoxDynError>
{
    let res = conn.fetch_all("").await?;

    let foo: Vec<FkInfo> = res.iter()
        .map(FkInfo::from)
        .collect();

    Ok(foo)
}

macro_rules! exit_on_err {
    ( $x:ident, $y:expr ) => {
        {
            if let Err(what) = $x {
                println!("ERROR: Could not connect to database:");
                println!("{what}");
                return;
            }
            $x.unwrap()
        }
    };
}

#[tokio::main]
async fn main() {
    let db_url = dotenv!("MYSQL_URL");
    println!("Connecting to database {db_url}");

    // Connect to information_schema and fetch all the FK constraints
    let information_schema = format!("{db_url}information_schema");
    println!("Fetching schema infos {information_schema}");
    let conn_res = MySqlConnection::connect(information_schema.as_str()).await;
    let mut conn = exit_on_err!(conn_res, "ERROR: Could not connect to database:");

    let fks = query_fk_constraints(&mut conn).await.unwrap();
    fks.iter().for_each(|fk| println!("FK found: {fk:?}"));

    let mkpool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(db_url).await;
    let pool = exit_on_err!(mkpool, "ERROR: Could not connect to database:");
}
