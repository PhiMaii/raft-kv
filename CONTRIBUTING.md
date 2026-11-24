# Contributing

raft-kv is a solo project, but it follows the same workflow a team project
would. This page describes it.

## Prerequisites

- Rust stable (pinned via `rust-toolchain.toml`; `rustup` installs it
  automatically)
- [`cargo-nextest`](https://nexte.st), [`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny)
  and [`mdbook`](https://rust-lang.github.io/mdBook/):

  ```sh
  cargo install --locked cargo-nextest cargo-deny mdbook
  ```

## Checks

Every change must pass the same checks CI runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --workspace --doc
cargo deny check
mdbook build docs
```

## Branches and pull requests

- `main` is always green. All work lands through pull requests.
- Branch names: `feat/<slug>`, `fix/<slug>`, `docs/<slug>`, `ci/<slug>`,
  `chore/<slug>`.
- One pull request per milestone step from the
  [specification](docs/src/spec.md#4-suggested-milestones-16-weeks).
- Pull requests are squash-merged. The pull request title becomes the commit
  subject on `main`, so it must follow the commit convention below.

## Commit messages

Commits follow [Conventional Commits](https://www.conventionalcommits.org):

```text
<type>(<scope>): <subject>

<body: why the change was made, if not obvious>
```

- **Types:** `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `ci`,
  `build`, `chore`
- **Scopes:** a crate name (`raft-core`, `storage`, `kv`, `sim`, …) or
  `deps`, `docs`, `repo`
- Subject in the imperative mood, no trailing period, at most 72 characters.

## Documentation

- Behaviour changes update `docs/src/` and, where relevant, the README.
- Significant design decisions get an ADR in `docs/src/adr/`.
- Every real bug found by a test is recorded in `docs/src/bugs.md` together
  with its regression test.
- User-visible changes are listed in `CHANGELOG.md` under `Unreleased`.
