use mysql_fk_fixer::args::parse_args;

pub mod utils;
use utils::exit_on_err;

fn main() {
    mysql_fk_fixer::run(exit_on_err!(parse_args(), "Could not parse CLI arguments"));
}
