//! Minimal raw-FH probe: mount, root fh, LOOKUP a path chain, READ a
//! file. Run as root: `cargo run -p migration-mover --example raw_smoke
//! -- nfs://server/export /some/dir/file`.

use migration_mover::libnfs::raw;
use migration_mover::libnfs::NfsContext;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let url = args.next().expect("usage: raw_smoke <nfs-url> <abs-path>");
    let path = args.next().expect("usage: raw_smoke <nfs-url> <abs-path>");

    eprintln!("mounting {url}");
    let mut ctx = NfsContext::mount_url(&url, 30_000)?;
    let root = raw::root_fh(&mut ctx).map_err(|e| anyhow::anyhow!(e.detail))?;
    eprintln!("root fh: {} bytes", root.len());

    let mut cur = root;
    let comps: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
    for (i, comp) in comps.iter().enumerate() {
        let t = std::time::Instant::now();
        cur = raw::lookup(&mut ctx, &cur, comp.as_bytes())
            .map_err(|e| anyhow::anyhow!("LOOKUP {comp}: {}", e.detail))?;
        eprintln!(
            "lookup[{i}] {comp}: fh {} bytes in {:?}",
            cur.len(),
            t.elapsed()
        );
    }

    let t = std::time::Instant::now();
    let (data, eof) =
        raw::read(&mut ctx, &cur, 0, 1 << 20).map_err(|e| anyhow::anyhow!("READ: {}", e.detail))?;
    eprintln!("read {} bytes eof={eof} in {:?}", data.len(), t.elapsed());
    Ok(())
}
