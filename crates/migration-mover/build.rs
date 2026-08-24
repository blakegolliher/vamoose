// Find libnfs via pkg-config and emit linker flags.
//
// This mirrors the approach used in `nfs-walker`. libnfs is
// LGPL-2.1-or-later and must be **dynamically** linked. Do not switch
// to static linking without first reviewing the license implications.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=VAMOOSE_LIBNFS_DIR");

    // Release bundles set this to a staging directory containing an exact,
    // digest-pinned libnfs and a `libnfs.so` linker alias. This prevents a
    // release from being linked against whatever mutable pkg-config entry
    // happens to be installed on the build host. Ordinary developer builds
    // retain the convenient pkg-config path below.
    if let Some(dir) = std::env::var_os("VAMOOSE_LIBNFS_DIR") {
        let dir = std::path::PathBuf::from(dir);
        let linker_name = dir.join("libnfs.so");
        assert!(
            linker_name.is_file(),
            "VAMOOSE_LIBNFS_DIR does not contain libnfs.so: {}",
            dir.display()
        );
        println!("cargo:rustc-link-search=native={}", dir.display());
        println!("cargo:rustc-link-lib=dylib=nfs");
        return;
    }

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
