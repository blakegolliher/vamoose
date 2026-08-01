use std::process::Command;

#[test]
fn missing_configuration_reports_the_existing_summary_and_exits_two() {
    let missing = std::env::temp_dir().join(format!(
        "vamoose-doctor-missing-config-{}-{}.toml",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    assert!(!missing.exists(), "test config path must not exist");

    let output = Command::new(env!("CARGO_BIN_EXE_vamoose"))
        .arg("--config")
        .arg(&missing)
        .arg("doctor")
        .output()
        .expect("run vamoose doctor");

    assert_eq!(output.status.code(), Some(2));
    let stdout = String::from_utf8(output.stdout).expect("doctor stdout is UTF-8");
    assert!(
        stdout.starts_with("vamoose doctor\n\nconfig: FAIL  reading config from "),
        "unexpected doctor report:\n{stdout}"
    );
    assert!(
        stdout.ends_with("\nSummary: 0 PASS, 0 WARN, 1 FAIL\n"),
        "missing existing failure summary:\n{stdout}"
    );
    assert!(
        output.stderr.is_empty(),
        "missing-config doctor previously wrote no stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
