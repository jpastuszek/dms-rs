extern crate clap;
use clap::{Arg, App};

fn main() {
	let matches = App::new("MyApp")
        .version("0.0")
        .author("JP")
        .about("Does awesome things")
        .arg(Arg::new("output")
			.help("Name")
			.index(1)
		)
        .get_matches();

    println!("Hello, world! {:?}", matches.value_of("output"));
}
