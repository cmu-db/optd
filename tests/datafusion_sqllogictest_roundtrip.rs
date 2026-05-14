#[test]
fn tpch_sqllogictest_contains_all_queries() {
    for query in 1..=22 {
        let path = format!("tests/slt/tpch/q{query:02}.slt");
        let script = std::fs::read_to_string(path).unwrap();
        assert!(
            script.contains(&format!("# TPC-H Q{query}")),
            "q{query:02}.slt should contain only TPC-H Q{query}"
        );
    }
}
