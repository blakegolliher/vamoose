//! Local half of the F43 schema-contract drift check.
//!
//! `SCHEMA_CONTRACT.md` (repo root, vendored byte-identical in
//! `nfs-walker`) is the single source of truth for the canonical
//! parquet schema. The cross-repo byte-diff runs in CI when the
//! upstream repo is reachable (see `.github/workflows/ci.yml`,
//! `contract-drift` job); THIS test is the always-on half: it parses
//! the contract's canonical column tables out of the markdown and
//! asserts `schema::canonical_schema()` and `schema::REQUIRED_COLUMNS`
//! agree with them, so code-vs-contract drift fails `cargo test` with
//! no network or token required.
//!
//! Parsing is deliberately dumb (line-oriented markdown table scrape):
//! if the contract's table format changes, this test fails loudly and
//! should be updated alongside it — that is drift detection working,
//! not a bug in the test.
//!
//! Lives in `src/` (compiled into the lib test binary) rather than
//! `tests/` deliberately: each `tests/*.rs` file links its own
//! full-workspace debug binary, and CI runners have been tipped into
//! linker SIGBUS (disk exhaustion) by exactly one binary too many.

use crate::schema;

/// A column row scraped from a contract markdown table:
/// (name, arrow type as written, nullable).
type ContractColumn = (String, String, bool);

fn contract_text() -> String {
    // migration-core sits at crates/migration-core; the contract is at
    // the repo root two levels up.
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../SCHEMA_CONTRACT.md");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// Slice `doc` between the `start` heading and the `end` heading.
fn section<'a>(doc: &'a str, start: &str, end: &str) -> &'a str {
    let s = doc
        .find(start)
        .unwrap_or_else(|| panic!("contract is missing the `{start}` heading"));
    let rest = &doc[s..];
    let e = rest
        .find(end)
        .unwrap_or_else(|| panic!("contract is missing the `{end}` heading after `{start}`"));
    &rest[..e]
}

/// Scrape `| `name` | `Type` | Yes/No | ...` rows from a section.
/// Header and separator rows don't start with `| \``, so they skip.
fn parse_columns(section: &str) -> Vec<ContractColumn> {
    let mut out = Vec::new();
    for line in section.lines() {
        let line = line.trim();
        if !line.starts_with("| `") {
            continue;
        }
        let cells: Vec<&str> = line.split('|').collect();
        assert!(
            cells.len() >= 5,
            "canonical column row has fewer cells than | name | type | nullable | definition |: {line}",
        );
        let name = cells[1].trim().trim_matches('`').to_string();
        let ty = cells[2].trim().trim_matches('`').to_string();
        let nullable = match cells[3].trim() {
            "Yes" => true,
            "No" => false,
            other => panic!("column `{name}`: unrecognized Nullable cell `{other}`"),
        };
        out.push((name, ty, nullable));
    }
    assert!(
        !out.is_empty(),
        "no column rows scraped — table format changed?"
    );
    out
}

/// First integer following `key` in `doc` (e.g. key = "`format_version = ").
fn version_after(doc: &str, key: &str) -> u32 {
    let idx = doc
        .find(key)
        .unwrap_or_else(|| panic!("contract is missing `{key}N`"));
    let digits: String = doc[idx + key.len()..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits
        .parse()
        .unwrap_or_else(|e| panic!("bad number after `{key}`: {e}"))
}

/// All canonical columns the contract defines: the two "Required
/// canonical columns" tables plus the "Optional canonical columns"
/// table. Legacy columns are excluded by construction (the slice stops
/// at the Legacy heading).
fn contract_canonical_columns(doc: &str) -> Vec<ContractColumn> {
    let required = section(
        doc,
        "## Required canonical columns",
        "## Optional canonical columns",
    );
    let optional = section(doc, "## Optional canonical columns", "## Legacy columns");
    let mut cols = parse_columns(required);
    cols.extend(parse_columns(optional));
    cols
}

/// Columns the contract marks non-nullable — i.e. the ones a shard can
/// never omit nor null out.
fn contract_non_nullable(doc: &str) -> Vec<String> {
    contract_canonical_columns(doc)
        .into_iter()
        .filter(|(_, _, nullable)| !nullable)
        .map(|(name, _, _)| name)
        .collect()
}

#[test]
fn canonical_schema_matches_contract_tables() {
    let doc = contract_text();
    let contract = contract_canonical_columns(&doc);
    let code = schema::canonical_schema();

    // Every contract column exists in code with the same arrow type
    // and nullability. `DataType`'s Debug form ("UInt64", "Binary",
    // "Utf8", ...) is exactly the spelling the contract tables use.
    for (name, ty, nullable) in &contract {
        let Some((idx, field)) = code.column_with_name(name) else {
            panic!("contract column `{name}` is missing from schema::canonical_schema()");
        };
        let code_ty = format!("{:?}", code.field(idx).data_type());
        assert_eq!(
            &code_ty, ty,
            "column `{name}`: contract says {ty}, canonical_schema() says {code_ty}",
        );
        assert_eq!(
            field.is_nullable(),
            *nullable,
            "column `{name}`: contract nullability {nullable}, canonical_schema() {}",
            field.is_nullable(),
        );
    }

    // And nothing extra: code must not grow canonical columns the
    // contract doesn't define (additive changes go through the
    // contract first — it is the source of truth).
    for field in code.fields() {
        assert!(
            contract.iter().any(|(name, _, _)| name == field.name()),
            "canonical_schema() column `{}` does not appear in the contract's canonical tables",
            field.name(),
        );
    }
}

#[test]
fn required_columns_match_contract_non_nullable_set() {
    // REQUIRED_COLUMNS drives `ShardReader::open` rejection
    // (Error::MissingColumn). The contract's non-nullable canonical
    // columns are precisely the set a shard can never function
    // without; pin the two against each other so neither drifts alone.
    let doc = contract_text();
    let mut contract: Vec<String> = contract_non_nullable(&doc);
    let mut code: Vec<String> = schema::REQUIRED_COLUMNS
        .iter()
        .map(|c| c.to_string())
        .collect();
    contract.sort();
    code.sort();
    assert_eq!(
        code, contract,
        "schema::REQUIRED_COLUMNS vs contract non-nullable canonical columns",
    );
}

#[test]
fn versions_match_contract() {
    let doc = contract_text();
    assert_eq!(
        schema::FORMAT_VERSION,
        version_after(&doc, "`format_version = "),
        "schema::FORMAT_VERSION vs the version this contract document describes",
    );
    assert_eq!(
        schema::CONTRACT_VERSION,
        version_after(&doc, "`contract_version = "),
        "schema::CONTRACT_VERSION vs the version this contract document describes",
    );
    assert_eq!(
        schema::CONTRACT_VERSION,
        version_after(&doc, "**Version:** "),
        "schema::CONTRACT_VERSION vs the contract header version",
    );
}
