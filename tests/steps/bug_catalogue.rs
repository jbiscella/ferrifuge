//! Steps of Block 49a: the bug-pattern catalogue, read from `CLAUDE.md` by
//! `tests/support/bug_catalogue.rs` and checked as its structural
//! acceptance says. Each check lists every problem it finds.

// cucumber passes captures by value.
#![allow(clippy::needless_pass_by_value)]

use std::collections::BTreeSet;
use std::path::Path;

use cucumber::{given, then};

use super::ClientWorld;
use crate::support::bug_catalogue::{self, Catalogue};

/// What a Block 49a scenario read.
#[derive(Debug, Default)]
pub(crate) struct BugCatalogue {
    catalogue: Option<Catalogue>,
    /// centrifuge-rust's §12 and §15 numbers.
    numbers: BTreeSet<u32>,
}

/// The repository: the crate's manifest directory.
fn repository() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn catalogue(world: &ClientWorld) -> &Catalogue {
    world
        .bug_catalogue
        .catalogue
        .as_ref()
        .expect("the catalogue read")
}

/// Fails with every problem of `problems`, one per line.
fn assert_none(what: &str, problems: Vec<String>) {
    assert!(problems.is_empty(), "{what}:\n  {}", problems.join("\n  "));
}

#[given("the bug-pattern catalogue in CLAUDE.md")]
fn the_catalogue(world: &mut ClientWorld) {
    let claude_md = std::fs::read_to_string(repository().join("CLAUDE.md"))
        .expect("CLAUDE.md")
        .replace("\r\n", "\n");
    world.bug_catalogue.catalogue = Some(Catalogue::parse(&claude_md));
}

#[given(
    "the bug-pattern catalogue in CLAUDE.md and the numbers of centrifuge-rust docs/spec.md section 12 at commit cbeac19"
)]
fn the_catalogue_and_its_numbers(world: &mut ClientWorld) {
    the_catalogue(world);
    world.bug_catalogue.numbers = bug_catalogue::numbers();
}

#[given("the probes of the bug-pattern catalogue")]
fn the_probes(world: &mut ClientWorld) {
    the_catalogue(world);
    assert!(
        !catalogue(world).probes.is_empty(),
        "the catalogue lists no probe"
    );
}

#[then(regex = r"^every number from (\d+) to (\d+) has exactly one row$")]
fn every_number_has_one_row(world: &mut ClientWorld, first: u32, last: u32) {
    let numbers = &world.bug_catalogue.numbers;
    assert_eq!(
        *numbers,
        (first..=last).collect::<BTreeSet<_>>(),
        "the numbers of §12 and §15"
    );
    assert_none(
        "numbers not in exactly one row",
        catalogue(world).numbers_not_in_one_row(numbers),
    );
}

#[then("every row's status is one of covered, gap, not applicable or not reconstructible")]
fn every_status_is_known(world: &mut ClientWorld) {
    assert_none("unknown statuses", catalogue(world).unknown_statuses());
}

#[then("every scenario a covered or gap row names is the name of a scenario in CLAUDE.md")]
fn every_scenario_exists(world: &mut ClientWorld) {
    assert_none("unknown scenarios", catalogue(world).unknown_scenarios());
}

#[then("every function a covered or gap row names exists at that path")]
fn every_function_exists(world: &mut ClientWorld) {
    assert_none(
        "missing functions",
        catalogue(world).missing_functions(repository()),
    );
}

#[then("every not applicable or not reconstructible row gives a reason")]
fn every_reason_given(world: &mut ClientWorld) {
    assert_none(
        "rows without a reason",
        catalogue(world).rows_without_reason(),
    );
}

#[then("every gap row names a probe scenario or a finding that is listed")]
fn every_gap_named(world: &mut ClientWorld) {
    assert_none(
        "gaps without a probe",
        catalogue(world).gaps_without_probe(),
    );
}

#[then("every commit an entry cites is an ancestor of HEAD")]
fn every_commit_an_ancestor(world: &mut ClientWorld) {
    let catalogue = catalogue(world);
    assert!(
        !catalogue.commits().is_empty(),
        "the catalogue cites no commit"
    );
    assert_none(
        "commits not ancestors of HEAD",
        catalogue.commits_not_ancestors(repository()),
    );
}

#[then("each names the mutation it was seen failing on")]
fn each_names_its_mutation(world: &mut ClientWorld) {
    assert_none(
        "probes without a mutation",
        catalogue(world).probes_without_mutation(),
    );
}

#[then("each names the pull request of that record, or is pending")]
fn each_names_its_record(world: &mut ClientWorld) {
    assert_none(
        "probes without a record",
        catalogue(world).probes_without_record(),
    );
}
