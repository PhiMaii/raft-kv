# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cargo workspace with one crate per component (`raft-core`, `storage`,
  `transport`, `kv`, `server`, `client`, `sim`, `chaos`, `bench`, `cli`).
- Documentation site (mdBook) with the specification, design overview,
  architecture decision records, testing methodology and bug log.
- CI: formatting, Clippy, tests, dependency audit and documentation build.
