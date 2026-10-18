//! Apollo Router binary with the `globo.subscription_snapshot` plugin.

// Linking the library registers the plugin in the Router plugin registry.
use router_subscription_snapshot as _;

fn main() -> anyhow::Result<()> {
    apollo_router::main()
}
