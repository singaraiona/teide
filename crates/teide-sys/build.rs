use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("../../src");
    let include_dir = manifest_dir.join("../../include");

    // Collect all .c files recursively under src/
    let c_files: Vec<PathBuf> = walkdir(&src_dir);

    let mut build = cc::Build::new();
    build
        .include(&include_dir)
        .include(&src_dir)
        .flag("-O3")
        .flag("-mtune=generic")
        .define("NDEBUG", None)
        .std("c17");

    for f in &c_files {
        build.file(f);
    }

    build.compile("teide");

    // Link system libraries on Linux/Unix
    if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=m");
        println!("cargo:rustc-link-lib=pthread");
    }

    println!("cargo:rerun-if-changed={}", src_dir.display());
    println!("cargo:rerun-if-changed={}", include_dir.display());
}

/// Recursively collect all `.c` files under `dir`.
fn walkdir(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walkdir(&path));
            } else if path.extension().map_or(false, |e| e == "c") {
                out.push(path);
            }
        }
    }
    out
}
