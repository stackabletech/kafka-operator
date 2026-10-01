//! Reusable, product-agnostic agent building blocks. Staged here until they move to operator-rs.

use clap::Args;
use stackable_operator::cli::CommonOptions;

/// Agent subcommands. Like [`stackable_operator::cli::Command`], but without `crd` as CRDs are only
/// deployed by operators.
#[derive(Debug, clap::Parser)]
pub enum AgentCommand<Run: Args = CommonOptions> {
    /// Run the agent.
    Run(Run),
}
