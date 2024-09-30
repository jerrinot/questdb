What is Cargo.unlocked? It is renamed to Cargo.lock when we want
to test with known-to-work versions of dependencies. Then Cargo
uses Cargo.lock to resolve dependencies.

If we want to test with the latest versions of dependencies -
for compatibility purposes - then Cargo.unlocked is simple ignored
and version ranges from Cargo.toml are used as prescribed by semver.