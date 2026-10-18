//! Apollo Router binary with the `realtime_sports.subscription_snapshot` plugin.

#[cfg(test)]
mod harness;
// Adding the module registers the plugin in the Router plugin registry.
mod snapshot;

fn main() -> anyhow::Result<()> {
    apollo_router::main()
}
