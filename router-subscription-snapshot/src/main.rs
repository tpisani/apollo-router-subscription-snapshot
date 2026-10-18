#[cfg(test)]
mod harness;
mod snapshot;

fn main() -> anyhow::Result<()> {
    apollo_router::main()
}
