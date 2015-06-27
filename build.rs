extern crate capnpc;
use std::env::var;
use std::fs;

fn main() {
    println!("{}", var("OUT_DIR").unwrap());
    println!("{}", var("OUT_DIR").unwrap() + "/messaging/schema");
    fs::create_dir_all(var("OUT_DIR").unwrap() + "/messaging/schema").unwrap();
    ::capnpc::compile("src", &["src/messaging/schema/raw_data_point.capnp"]).ok().expect("Failed to compile Cap'n Proto schema");
}

