extern crate cc;

use std::path;

fn main() {
    if !path::Path::new("odpi_src/include/dpi.h").exists() {
        println!("The odpi submodule isn't initialized. Run the following commands.");
        println!("  git submodule init");
        println!("  git submodule update");
        std::process::exit(1);
    }
    
    // let mut build = cc::Build::new();
    // for entry in fs::read_dir("odpi/src").unwrap() {
    //     let fname = entry.unwrap().file_name().into_string().unwrap();
    //     if fname.ends_with(".c") {
    //         build.file(format!("odpi/src/{}", fname));
    //     }
    // }
    
    cc::Build::new()
        .file("odpi_src/embed/dpi.c")
        .include("odpi_src/include")
        .flag_if_supported("-Wno-unused-parameter")
        .compile("libodpic.a");
}
