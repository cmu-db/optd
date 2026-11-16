//! CLI demos must never replace an existing user-supplied catalog file.
use std::process::Command;

#[test]
fn statistics_demo_preserves_existing_files() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("existing.sqlite");
    let original = b"existing user data must not be removed";
    std::fs::write(&path, original).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_catalog-stats-demo"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "existing catalogs must be refused, not reset"
    );
    assert_eq!(std::fs::read(path).unwrap(), original);
}

#[test]
fn statistics_demo_does_not_decode_literal_file_names() {
    let temp = tempfile::tempdir().unwrap();
    let neighbor = temp.path().join("catalog.db");
    std::fs::write(&neighbor, b"").unwrap();
    for name in [
        "catalog%2Edb",
        "file:catalog.db",
        "catalog?mode=ro#literal.db",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_catalog-stats-demo"))
            .current_dir(temp.path())
            .arg(name)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            std::fs::read(&neighbor).unwrap().is_empty(),
            "{name}: a literal filename must not redirect to a different existing file"
        );
        assert!(!std::fs::read(temp.path().join(name)).unwrap().is_empty());
    }
}

#[test]
fn statistics_demo_creates_a_fresh_catalog_without_allowing_implicit_reset() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fresh.sqlite");
    let output = Command::new(env!("CARGO_BIN_EXE_catalog-stats-demo"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("persisted row count: Some(3)"));
    let original = std::fs::read(&path).unwrap();
    let second = Command::new(env!("CARGO_BIN_EXE_catalog-stats-demo"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert_eq!(std::fs::read(path).unwrap(), original);
}
