/// Run expression returning a Result<>, If Err() logs the error and return; else unwrap()
/// Usage let result = exit_on_err!(try_do(), "Try do failed");
macro_rules! exit_on_err {
    ( $x:expr, $y:expr ) => {
        {
            let var = $x;
            if let Err(what) = var {
                eprintln!("ERROR: {}", $y);
                eprintln!("{what}");
                return;
            }
            var.unwrap()
        }
    };
}
pub(crate) use exit_on_err;

/// Run expression returning a Result<>, If Err() logs the error and return; else unwrap()
/// Just like exit_on_err, except it's made for loops
#[allow(unused_macros)]
macro_rules! continue_on_err {
    ( $x:expr, $y:expr ) => {
        {
            let var = $x;
            if let Err(what) = var {
                eprintln!("ERROR: {}", $y);
                eprintln!("{what}");
                continue;
            }
            var.unwrap()
        }
    };
}
#[allow(unused_imports)]
pub(crate) use continue_on_err;
