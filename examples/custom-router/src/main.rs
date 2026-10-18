//! A custom Apollo Router binary: the official Router plus the native plugins it links.

// Linking the crate is enough: `register_plugin!` adds `globo.subscription_snapshot`
// to the Router plugin registry. Add a line like this for each plugin.
use subscription_snapshot as _;

fn main() -> anyhow::Result<()> {
    apollo_router::main()
}
