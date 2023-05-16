use std::path::PathBuf;

use mysql::{Conn, Opts, Result};

pub mod args;
use args::{parse_args, AppArgs, Command, CheckFkArgs};

pub mod fk;
use fk::FkInfo;

pub mod fkchecker;
use fkchecker::FkChecker;

pub mod datadumper;

pub mod tabledumper;

pub mod utils;
use utils::{exit_on_err, continue_on_err};

fn get_conn(args: &AppArgs) -> Result<Conn> {
    println!("Connecting to {}", args.db_url);
    let res = Conn::new(Opts::from_url(args.db_url.as_str())?)?;

    let version_numbers = res.server_version();
    println!("MySQL server version: {}.{}.{}", version_numbers.0, version_numbers.1, version_numbers.2);
    Ok(res)
}

fn check_fks(mut conn: &mut Conn, schema: Option<&String>, args: CheckFkArgs) {
    let fk_constraints = exit_on_err!(FkInfo::query_fk_constraints(conn, schema), "Could not get list of FK constraints");

    println!("Found {} Foreign Key Constraints to check...", fk_constraints.len());

    let checker = exit_on_err!(FkChecker::new(args.auto_delete, args.dump_invalid_rows, args.dump_loc), "Could not initialise FK checker");
    for fk in fk_constraints {
        println!("Checking Foreign Key constraint {fk}");
        let res = checker.check::<u32>(&fk, &mut conn);
        let res = continue_on_err!(res, "Could not check Foreign Key Constraint");
        if res.len() > 0 {
            println!("{} invalid foreign references found in table {} column {}", res.len(), fk.table, fk.column);
        }
    }
}

fn dump_all(conn: &mut Conn, schema: Option<&String>, _: Option<PathBuf>) {
    exit_on_err!(tabledumper::dump_all_tables(conn, schema.expect("Schema is mandatory")), "Dump all op failed");
}

fn run(args: AppArgs) {
    let mut conn = exit_on_err!(get_conn(&args), "Could not connect to MySQL");

    match args.command {
        Command::CheckFk(check_fk_args) => check_fks(&mut conn, args.schema.as_ref(), check_fk_args),
        Command::DumpAll(loc) => dump_all(&mut conn, args.schema.as_ref(), loc),
    }
}

fn main() {
    run(exit_on_err!(parse_args(), "Could not parse CLI arguments"));
}
