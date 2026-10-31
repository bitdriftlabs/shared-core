# Package Licenses

`shared-core` is a mixed-license repository. Each Cargo package is licensed independently; its
`[package]` metadata is the machine-readable authority. Apache License 2.0 is in
[`LICENSE.apache`](LICENSE.apache) and PolyForm Strict 1.0.0 is in
[`LICENSE.polyform`](LICENSE.polyform).

## Apache License 2.0

| Package | License |
| --- | --- |
| `bd-backoff` | Apache-2.0 |
| `bd-grpc` | Apache-2.0 |
| `bd-grpc-codec` | Apache-2.0 |
| `bd-log` | Apache-2.0 |
| `bd-log-util` | Apache-2.0 |
| `bd-panic` | Apache-2.0 |
| `bd-pgv` | Apache-2.0 |
| `bd-rt` | Apache-2.0 |
| `bd-runtime-config` | Apache-2.0 |
| `bd-server-stats` | Apache-2.0 |
| `bd-shutdown` | Apache-2.0 |
| `bd-stats-common` | Apache-2.0 |
| `bd-test-helpers-core` | Apache-2.0 |
| `bd-time` | Apache-2.0 |
| `bd-workspace-hack` | Apache-2.0 |

## PolyForm Strict 1.0.0

Every other Cargo package in this repository is licensed under PolyForm Strict 1.0.0 unless its
package metadata and this map explicitly say otherwise.

Previously published versions retain their original licenses. Third-party components and
submodules retain their own licenses and notices.

The separately versioned first-party API definitions and tooling in `api/` use PolyForm Strict
1.0.0; see [`api/LICENSE`](api/LICENSE). Its vendored components retain their own licenses.
