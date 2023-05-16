use std::path::PathBuf;

use pico_args::Arguments;
use pico_args::Error;

#[derive(Debug)]
pub struct AppArgs {
    pub db_url: String,
    pub command: Command,
    pub schema: Option<String>,
}

#[derive(Debug)]
pub enum Command {
    CheckFk(CheckFkArgs),
    DumpAll(Option<PathBuf>),
}

#[derive(Debug)]
pub struct CheckFkArgs {
    pub auto_delete: bool,
    pub dump_invalid_rows: bool,
    pub dump_loc: Option<PathBuf>,
}

impl Command {
    #[inline]
    pub fn is_check_fk(&self) -> bool {
        matches!(*self, Command::CheckFk(_))
    }
    #[inline]
    pub fn is_dump_all(&self) -> bool {
        matches!(*self, Command::DumpAll(_))
    }
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

fn parse_check_fk_args(pargs: &mut Arguments) -> Result<CheckFkArgs, Error> {
    let res = CheckFkArgs {
        auto_delete: pargs.contains("--auto-delete"),
        dump_invalid_rows: pargs.contains("--dump-invalid-rows"),
        dump_loc: parse_dump_loc(pargs)?,
    };
    Ok(res)
}

fn parse_command(pargs: &mut Arguments) -> Result<Command, Error> {
    let command = pargs.free_from_str::<String>()?;
    match command.as_str() {
        "find-invalid-foreign-refs" => Ok(Command::CheckFk(parse_check_fk_args(pargs)?)),
        "dump-all-tables" => Ok(Command::DumpAll(parse_dump_loc(pargs)?)),
        _ => Err(Error::ArgumentParsingFailed { cause: format!("Unknown command: {command}") })
    }
}

pub fn parse_args() -> Result<AppArgs, pico_args::Error> {
    _parse_args(Arguments::from_env())
}

fn _parse_args(mut pargs: Arguments) -> Result<AppArgs, pico_args::Error> {
    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        println!("Usage: mysql_fk_checker <Database URL> find-invalid-foreign-refs [schema name]");
        std::process::exit(0);
    }

    let args = AppArgs {
        db_url: pargs.free_from_str()?,
        schema: pargs.opt_value_from_str::<_, String>("--schema")?,
        command: parse_command(&mut pargs)?,
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
        if let Command::CheckFk(check_fk_args) = res.command {
            assert!(false == check_fk_args.auto_delete);
            assert!(false == check_fk_args.dump_invalid_rows);
            assert!(check_fk_args.dump_loc.is_none());
        } else {
            panic!("command should be find-invalid-foreign-refs");
        }
    }

    #[test]
    fn parse_args_dump_all() {
        let args: Vec<OsString> = vec![
            "mysql://root:password@localhost/".into(),
            "dump-all-tables".into(),
        ];
        let res = _parse_args(Arguments::from_vec(args)).expect("parse op unsuccessful");
        assert!(res.command.is_dump_all());
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
        if let Command::CheckFk(check_fk_args) = res.command {
            assert!(true == check_fk_args.auto_delete);
            assert!(true == check_fk_args.dump_invalid_rows);
            assert_eq!(check_fk_args.dump_loc.expect("missing dump-folder").file_name().unwrap(), OsString::from_str("target").unwrap());
        } else {
            panic!("command should be find-invalid-foreign-refs");
        }
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
