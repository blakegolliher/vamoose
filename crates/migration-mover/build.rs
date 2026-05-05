// Find libnfs via pkg-config and emit linker flags.
//
// This mirrors the approach used in `nfs-walker`. libnfs is
// LGPL-2.1-or-later and must be **dynamically** linked. Do not switch
// to static linking without first reviewing the license implications.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let lib = pkg_config::Config::new()
        .atleast_version("4.0.0")
        .probe("libnfs")
        .expect("libnfs not found via pkg-config; install libnfs-dev / libnfs-devel");

    for path in &lib.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }
    for name in &lib.libs {
        // dylib, not static — see comment above.
        println!("cargo:rustc-link-lib=dylib={}", name);
    }
}
