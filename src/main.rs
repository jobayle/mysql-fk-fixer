use mysql::{Conn, Opts};
use dotenvy_macro::dotenv;

pub mod fk;
use fk::{FkInfo, FkChecker};

pub mod utils;
use utils::{exit_on_err, continue_on_err};

fn main() {
    let url = dotenv!("MYSQL_URL");
    let auto_delete = exit_on_err!(dotenv!("AUTO_DELETE").trim().parse::<bool>(), "Could not parse AUTO_DELETE, allowed values: true | false");
    let dump_invalid_rows = exit_on_err!(dotenv!("DUMP_INVALID_ROWS").trim().parse::<bool>(), "Could not parse DUMP_INVALID_ROWS, allowed values: true | false");
    let dump_location = String::from(dotenv!("DUMP_FOLDER"));

    println!("Connecting to {url}");
    let opts = exit_on_err!(Opts::from_url(url), "Could not parse connection URL");
    let mut conn = exit_on_err!(Conn::new(opts), "Could not connect to MySQL");

    let version_numbers = conn.server_version();
    println!("MySQL server version: {}.{}.{}", version_numbers.0, version_numbers.1, version_numbers.2);

    let fk_constraints = exit_on_err!(FkInfo::query_fk_constraints(&mut conn), "Could not get list of FK constraints");

    println!("Found {} Foreign Key Constraints to check...", fk_constraints.len());

    let checker = exit_on_err!(FkChecker::new(auto_delete, dump_invalid_rows, dump_location), "Could not initialise FK checker");
    for fk in fk_constraints {
        println!("Checking Foreign Key constraint {fk}");
        let res = checker.check::<u32>(&fk, &mut conn);
        let res = continue_on_err!(res, "Could not check Foreign Key Constraint");
        if res.len() > 0 {
            println!("{} invalid foreign references found in table {} column {}", res.len(), fk.table, fk.column);
        }
    }
}
