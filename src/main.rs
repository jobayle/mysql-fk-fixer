use mysql::{Conn, Opts};
use dotenvy_macro::dotenv;

pub mod fk;
use fk::{FkInfo, FkChecker};

pub mod utils;
use utils::{exit_on_err, continue_on_err};

fn main() {
    let base_url = dotenv!("MYSQL_URL");
    let url = format!("{base_url}information_schema");
    println!("Connecting to {url}");
    let opts = exit_on_err!(Opts::from_url(url.as_str()), "Could not parse connection URL");
    let mut conn = exit_on_err!(Conn::new(opts), "Could not connect to MySQL");

    let version_numbers = conn.server_version();
    println!("MySQL server version: {}.{}.{}", version_numbers.0, version_numbers.1, version_numbers.2);

    let fk_constraints = exit_on_err!(FkInfo::query_fk_constraints(&mut conn), "Could not get list of FK constraints");

    println!("Found {} Foreign Key Constraints to check...", fk_constraints.len());
    //fk_constraints.iter().for_each(|fk| println!("fk: {fk:?}")) // DBG
    for fk in fk_constraints {
        conn.select_db(&fk.schema);
        println!("Checking Foreign Key constraint {fk}");
        let res = FkChecker::check(&fk, &mut conn);
        let res = continue_on_err!(res, "Could not check Foreign Key Constraint");
        if res.len() > 0 {
            println!("{} invalid foreign references found in table {} column {}", res.len(), fk.table, fk.column);
        }
    }
}
