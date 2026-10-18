#[cfg(test)]
mod harness;
mod retain;

fn main() -> anyhow::Result<()> {
    apollo_router::main()
}
