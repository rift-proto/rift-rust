#![allow(dead_code, unused_imports, unused_variables, clippy::all)]
//! Update the benchmark results section in README.adoc.
//!
//! Scans `target/criterion/** /new/estimates.json` (Criterion 0.5 output) and
//! replaces the content between `// BENCHMARK_RESULTS:BEGIN` and
//! `// BENCHMARK_RESULTS:END` markers with a sorted AsciiDoc table.
//!
//! ## Usage
//!
//! ```sh
//! cargo run --example update_benchmark_readme -- README.adoc
//! cargo run --example update_benchmark_readme -- --check README.adoc
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const MARKER_BEGIN: &str = "// BENCHMARK_RESULTS:BEGIN";
const MARKER_END: &str = "// BENCHMARK_RESULTS:END";

#[derive(Debug)]
struct BenchEstimate {
    mean: f64,
    median: f64,
    mean_lower: f64,
    mean_upper: f64,
}

fn collect_estimates(criterion_dir: &Path) -> BTreeMap<String, BenchEstimate> {
    let mut results = BTreeMap::new();

    fn walk(dir: &Path, results: &mut BTreeMap<String, BenchEstimate>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "new") {
                    let estimates_path = path.join("estimates.json");
                    if estimates_path.exists()
                        && let Some((name, estimate)) = parse_benchmark(&path)
                    {
                        results.insert(name, estimate);
                    }
                } else {
                    walk(&path, results);
                }
            }
        }
    }

    walk(criterion_dir, &mut results);
    results
}

fn parse_benchmark(new_dir: &Path) -> Option<(String, BenchEstimate)> {
    let estimates_path = new_dir.join("estimates.json");
    let content = fs::read_to_string(&estimates_path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&content).ok()?;

    let mean = json.get("mean")?.get("point_estimate")?.as_f64()?;
    let median = json.get("median")?.get("point_estimate")?.as_f64()?;
    let mean_lower = json
        .get("mean")?
        .get("confidence_interval")?
        .get("lower_bound")?
        .as_f64()?;
    let mean_upper = json
        .get("mean")?
        .get("confidence_interval")?
        .get("upper_bound")?
        .as_f64()?;

    // Derive bench name from path:
    // target/criterion/<group>/<bench_id>/new -> <group>/<bench_id>
    let parent = new_dir.parent()?;
    let bench_id = parent.file_name()?.to_str()?.to_string();

    // Walk up to find group name
    // Path: target/criterion/<group>/<bench_id>/new
    let grandparent = parent.parent()?;
    // Skip if grandparent is criterion_dir itself (flat bench)
    let group: PathBuf = if grandparent.file_name().is_some_and(|n| n == "criterion") {
        // Flat: bench_id is actually the group
        // Path: target/criterion/<group>/new
        // So parent is the group, and there's no bench_id
        // Actually this case shouldn't happen with our setup, but handle it
        return None;
    } else {
        grandparent.to_path_buf()
    };

    let group_name = group.file_name()?.to_str()?;
    let name = if bench_id == group_name {
        group_name.to_string()
    } else {
        format!("{group_name}/{bench_id}")
    };

    Some((
        name,
        BenchEstimate {
            mean,
            median,
            mean_lower,
            mean_upper,
        },
    ))
}

/// Criterion point_estimate values are in nanoseconds.
fn format_duration(nanos: f64) -> String {
    if nanos >= 1_000_000_000.0 {
        format!("{:.3} s", nanos / 1_000_000_000.0)
    } else if nanos >= 1_000_000.0 {
        format!("{:.3} ms", nanos / 1_000_000.0)
    } else if nanos >= 1_000.0 {
        format!("{:.3} µs", nanos / 1_000.0)
    } else {
        format!("{:.3} ns", nanos)
    }
}

fn build_table(results: &BTreeMap<String, BenchEstimate>) -> String {
    let mut out = String::new();
    out.push_str(".Benchmark results (lower is better)\n");
    out.push_str("[cols=\"4,2,2,2\",options=\"header\"]\n");
    out.push_str("|===\n");
    out.push_str("| Benchmark | Mean | Median | CI (mean)\n");
    for (name, est) in results {
        let ci = format!(
            "{} – {}",
            format_duration(est.mean_lower),
            format_duration(est.mean_upper),
        );
        out.push_str(&format!(
            "| `{name}`\n| {}\n| {}\n| {ci}\n",
            format_duration(est.mean),
            format_duration(est.median),
        ));
    }
    out.push_str("|===\n");
    out
}

fn read_readme(path: &Path) -> io::Result<String> {
    fs::read_to_string(path)
}

fn update_readme_section(original: &str, results: &BTreeMap<String, BenchEstimate>) -> String {
    let table = build_table(results);
    let header = format!(
        "{MARKER_BEGIN}\n// Generated by: cargo run --example update_benchmark_readme -- README.adoc"
    );
    let footer = MARKER_END;

    // If markers don't exist, append the section at the end
    if !original.contains(MARKER_BEGIN) {
        let mut new = original.to_owned();
        if !new.is_empty() && !new.ends_with('\n') {
            new.push('\n');
        }
        new.push('\n');
        new.push_str("== Benchmarks\n\n");
        new.push_str(&header);
        new.push('\n');
        new.push_str(&table);
        new.push_str(footer);
        new.push('\n');
        return new;
    }

    let mut result = String::new();
    let mut in_block = false;
    for line in original.lines() {
        if line.trim() == MARKER_BEGIN {
            in_block = true;
            result.push_str(&header);
            result.push('\n');
            result.push_str(&table);
            continue;
        }
        if in_block && line.trim() == MARKER_END {
            in_block = false;
            result.push_str(footer);
            result.push('\n');
            continue;
        }
        if in_block {
            // skip old content
            continue;
        }
        result.push_str(line);
        result.push('\n');
    }
    result
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: {} [--check] [--full] <README.adoc>",
            args.first()
                .map_or("update_benchmark_readme", |s| s.as_str()),
        );
        eprintln!(
            "  Collects `target/criterion/**/new/estimates.json` and updates the README marker block."
        );
        eprintln!("  --check   Exit 1 if the README would change (dry-run).");
        eprintln!("  --full    Include all benchmarks (default: include all).");
        std::process::exit(1);
    }

    let mut check_only = false;

    let positional: Vec<_> = args
        .iter()
        .skip(1)
        .filter(|a| {
            if *a == "--check" {
                check_only = true;
                false
            } else if *a == "--full" {
                // currently no-op, always include all
                false
            } else {
                true
            }
        })
        .collect();

    if positional.is_empty() {
        eprintln!("error: missing README path argument");
        std::process::exit(1);
    }
    let readme_path: &str = positional[0].as_str();

    let criterion_dir = Path::new("target").join("criterion");
    let results = collect_estimates(&criterion_dir);

    if results.is_empty() {
        eprintln!(
            "No benchmark results found in {}. Run `cargo bench` first.",
            criterion_dir.display(),
        );
        std::process::exit(1);
    }

    let original = read_readme(Path::new(readme_path)).unwrap_or_else(|e| {
        eprintln!("error reading {readme_path}: {e}");
        std::process::exit(1);
    });

    let updated = update_readme_section(&original, &results);

    if updated == original {
        println!("README is up to date ({readme_path})");
        return;
    }

    if check_only {
        eprintln!(
            "README is stale ({readme_path}). Run: cargo run --example update_benchmark_readme -- {readme_path}"
        );
        std::process::exit(1);
    }

    fs::write(readme_path, &updated).unwrap_or_else(|e| {
        eprintln!("error writing {readme_path}: {e}");
        std::process::exit(1);
    });
    println!(
        "Updated benchmark results in {readme_path} ({results_len} benchmarks)",
        results_len = results.len()
    );
}
