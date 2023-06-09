use mysql::{Conn, Opts, Result};

pub mod args;
use args::AppArgs;

pub mod fk;
use fk::{FkInfo, FkIndex};

pub mod fkchecker;
use fkchecker::FkChecker;

pub mod datadumper;

#[macro_use]
pub mod utils;
use utils::{exit_on_err, continue_on_err};

fn get_conn(args: &AppArgs) -> Result<Conn> {
    println!("Connecting to {}", args.db_url);
    let res = Conn::new(Opts::from_url(args.db_url.as_str())?)?;

    let version_numbers = res.server_version();
    println!("MySQL server version: {}.{}.{}", version_numbers.0, version_numbers.1, version_numbers.2);
    Ok(res)
}

fn check_fks(mut conn: &mut Conn, args: AppArgs) {
    let fk_constraints = FkIndex::from(exit_on_err!(FkInfo::query_fk_constraints(conn, args.schema.as_ref()), "Could not get list of FK constraints"));

    println!("Found {} Foreign Key Constraints to check...", fk_constraints.fks.len());

    let checker = exit_on_err!(FkChecker::new(args.auto_delete, args.dump_invalid_rows, args.dump_loc), "Could not initialise FK checker");
    for fk in fk_constraints.fks.iter() {
        println!("Checking Foreign Key constraint {fk}");
        let res = checker.check::<u32, Conn>(&fk, &fk_constraints, &mut conn);
        let res = continue_on_err!(res, "Could not check Foreign Key Constraint");
        if res.len() > 0 {
            println!("{} invalid foreign references found in table {} column {}", res.len(), fk.table, fk.column);
        }
    }
}

pub fn run(args: AppArgs) {
    let mut conn = exit_on_err!(get_conn(&args), "Could not connect to MySQL");
    check_fks(&mut conn, args);
}
