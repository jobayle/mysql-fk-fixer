use std::path::PathBuf;

use pico_args::Arguments;
use pico_args::Error;

#[derive(Debug)]
pub struct AppArgs {
    pub db_url: String,
    pub auto_delete: bool,
    pub dump_invalid_rows: bool,
    pub dump_loc: Option<PathBuf>,
    pub schema: Option<String>,
}

fn parse_dump_loc(pargs: &mut Arguments) -> Result<Option<PathBuf>, Error> {
    let path_str = pargs.opt_value_from_str::<_, String>("--dump-folder")?
        .map(PathBuf::from);
    if path_str.is_some() {
        let val = path_str.as_ref().unwrap();
        if val.exists() && !val.is_dir() {
            return Err(Error::ArgumentParsingFailed { cause: format!("Dump folder {} is not a folder", val.display()) });
        }
    }
    Ok(path_str)
}

pub fn parse_args() -> Result<AppArgs, pico_args::Error> {
    _parse_args(Arguments::from_env())
}

fn _parse_args(mut pargs: Arguments) -> Result<AppArgs, pico_args::Error> {
    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        println!("Usage: mysql_fk_checker <Database URL> [schema name]");
        std::process::exit(0);
    }

    let args = AppArgs {
        db_url: pargs.free_from_str()?,
        schema: pargs.opt_value_from_str::<_, String>("--schema")?,
        auto_delete: pargs.contains("--auto-delete"),
        dump_invalid_rows: pargs.contains("--dump-invalid-rows"),
        dump_loc: parse_dump_loc(&mut pargs)?,
    };
    Ok(args)
}

#[cfg(test)]
mod test {
    use std::{ffi::OsString, str::FromStr};

    use super::*;

    #[test]
    fn parse_args_check_fk() {
        let args: Vec<OsString> = vec![
            "mysql://root:password@localhost/".into(),
            "find-invalid-foreign-refs".into(),
        ];
        let res = _parse_args(Arguments::from_vec(args)).expect("parse op unsuccessful");

        assert!(false == res.auto_delete);
        assert!(false == res.dump_invalid_rows);
        assert!(res.dump_loc.is_none());
    }

    #[test]
    fn parse_args_schema() {
        let args: Vec<OsString> = vec![
            "mysql://root:password@localhost/".into(),
            "--schema".into(),
            "my_schema".into(),
            "dump-all-tables".into(),
        ];
        let res = _parse_args(Arguments::from_vec(args)).expect("parse op unsuccessful");
        assert_eq!(res.schema.expect("missing schema"), "my_schema");
    }

    #[test]
    fn parse_args_check_fk_options() {
        let args: Vec<OsString> = vec![
            "mysql://root:password@localhost/".into(),
            "find-invalid-foreign-refs".into(),
            "--auto-delete".into(),
            "--dump-invalid-rows".into(),
            "--dump-folder".into(),
            "target".into(),
        ];
        let res = _parse_args(Arguments::from_vec(args)).expect("parse op unsuccessful");
        assert!(true == res.auto_delete);
        assert!(true == res.dump_invalid_rows);
        assert_eq!(res.dump_loc.expect("missing dump-folder").file_name().unwrap(), OsString::from_str("target").unwrap());
    }

    #[test]
    fn parse_args_dump_all_not_a_dir() {
        let args: Vec<OsString> = vec![
            "mysql://root:password@localhost/".into(),
            "dump-all-tables".into(),
            "--dump-folder".into(),
            "Cargo.toml".into(),
        ];
        let res = _parse_args(Arguments::from_vec(args)).expect_err("parse op successful");
        assert!(res.to_string().contains("is not a folder"));
    }
}
