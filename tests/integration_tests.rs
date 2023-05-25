use std::fs;
use std::path::{PathBuf, Path};

use mysql::prelude::*;
use mysql::{Conn, Opts};
use mysql_fk_fixer::run;
use mysql_fk_fixer::args::AppArgs;

const DB_URL: &'static str = "mysql://root:root@localhost/";

fn clean_dump_folder(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("path should be removeable");
    }
}

fn get_conn() -> Conn {
    Conn::new(Opts::from_url(DB_URL).unwrap()).expect("Cannot connect to local MySQL DB using account root:root")
}

fn setup_db(conn: &mut Conn, schema: &String) {
    conn.query_drop(format!("DROP DATABASE IF EXISTS {schema};")).expect("DROP DB unsuccessful");
    conn.query_drop(format!("CREATE DATABASE {schema};")).expect("CREATE DB unsuccessful");
    conn.query_drop(format!("USE {schema};")).expect("USE DB unsuccessful");
    let script = fs::read_to_string("tests/invalid_foreign_ref.sql").unwrap();
    conn.query_drop(script).expect("invalid_foreign_ref.sql should be successful")
}

#[test]
fn it_run_dump() {
    let dump_folder = PathBuf::from("it_dumps");
    clean_dump_folder(&dump_folder);
    let args = AppArgs {
        auto_delete: false,
        dump_invalid_rows: true,
        dump_loc: Some(dump_folder.clone()),
        db_url: String::from(DB_URL),
        schema: Some(String::from("dump"))
    };
    setup_db(&mut get_conn(), args.schema.as_ref().unwrap());

    run(args);

    assert!(dump_folder.exists());
    let dump_file = dump_folder.join("baz_ibfk_2.csv");
    assert!(dump_file.exists());
    // TODO check content of dump
    clean_dump_folder(&dump_folder);
}

#[test]
fn it_run_delete_all() {
    let args = AppArgs {
        auto_delete: true,
        dump_invalid_rows: false,
        dump_loc: None,
        db_url: String::from(DB_URL),
        schema: Some(String::from("del"))
    };
    setup_db(&mut get_conn(), args.schema.as_ref().unwrap());

    run(args);
    // TODO check row containing invalid foreign ref has properly been removed
}
