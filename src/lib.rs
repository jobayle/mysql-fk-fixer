use std::sync::Arc;
use tokio::task::JoinHandle;
use mysql::{Pool, PooledConn, Opts, Result};

pub mod args;
use args::AppArgs;

pub mod fk;
use fk::{FkInfo, FkIndex};

pub mod fkchecker;
use fkchecker::FkChecker;

pub mod datadumper;

#[macro_use]
pub mod utils;
use utils::exit_on_err;

fn get_conn(args: &AppArgs) -> Result<Pool> {
    println!("Connecting to {}", args.db_url);
    let pool = Pool::new(Opts::from_url(args.db_url.as_str())?)?;
    let conn = pool.get_conn()?;

    let version_numbers = conn.server_version();
    println!("MySQL server version: {}.{}.{}", version_numbers.0, version_numbers.1, version_numbers.2);
    Ok(pool)
}

#[tokio::main]
async fn check_fks(pool: &mut Pool, args: AppArgs) {
    let mut conn = exit_on_err!(pool.get_conn(), "Could not get connection from pool");
    let fk_constraints = Arc::new(FkIndex::from(exit_on_err!(FkInfo::query_fk_constraints(&mut conn, args.schema.as_ref()), "Could not get list of FK constraints")));

    println!("Found {} Foreign Key Constraints to check...", fk_constraints.fks.len());

    let checker = Arc::new(exit_on_err!(FkChecker::new(args.auto_delete, args.dump_invalid_rows, args.dump_loc), "Could not initialise FK checker"));
    let join_handles = fk_constraints.fks.iter().map(|fk| {
        let fk_constraints = fk_constraints.clone();
        let checker = checker.clone();
        let mut conn = pool.get_conn().unwrap();
        let fk = fk.clone();
        tokio::spawn(async move {
            println!("Checking Foreign Key constraint {fk}");
            let res = checker.check::<u32, PooledConn>(&fk, &fk_constraints, &mut conn);
            let res = exit_on_err!(res, "Could not check Foreign Key Constraint");
            if res.len() > 0 {
                println!("{} invalid foreign references found in table {} column {}", res.len(), fk.table, fk.column);
            }
        })
     })
    .collect::<Vec<JoinHandle<()>>>();

    // Is it necessary to join ? what happens when the main thread ends?
    for jh in join_handles {
        if let Err(e) = jh.await {
            eprintln!("ERROR: join failed: {}", e);
        }
    }
}

pub fn run(args: AppArgs) {
    let mut conn = exit_on_err!(get_conn(&args), "Could not connect to MySQL");
    check_fks(&mut conn, args);
}
