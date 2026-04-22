---
name: update-api
description: Update the api submodule and regenerate bd-proto artifacts. Use this skill whenever the user asks to sync API definitions, refresh generated protobuf code, handle newly exported public API protos, fix missing bd-proto module wiring after an API bump, or open a PR for API updates.
---

# Update api and regenerate protos

Use this workflow to safely update the `api` submodule, regenerate checked-in protobuf artifacts,
teach `bd-proto` about newly exported public API protos, and validate both public API compilation
paths.

## Expected output

Always end with:
1. the old and new `api` submodule SHA,
2. a concise list of changed files grouped by type (submodule pointer, generated Rust, config,
   module wiring, other intentional files),
3. the PR URL or an explicit error that blocked PR creation.

## Preconditions

- Run from the `shared-core` repository root.
- Do not use `SKIP_PROTO_GEN=1`; generation must run through `cargo build`.
- In this CLI environment, avoid shell expansion patterns in generated commands. Do not emit or use
  command substitution, indirect expansion, or `${...}`-style shell variables.
- Before creating a PR, make sure the branch diff is scoped to the API bump and the regeneration
  work done in this run.

## Workflow

1. Capture the current branch and the current `api` submodule SHA.
   - `git rev-parse --abbrev-ref HEAD`
   - `git -C api rev-parse HEAD`

2. Update the submodule to the desired revision.
   - If the user named a target commit or branch, move `api` there.
   - Otherwise sync to the latest `main`:
     - `git submodule update --init api`
     - `git -C api fetch origin main`
     - `git -C api checkout --detach origin/main`
   - Capture the new SHA with `git -C api rev-parse HEAD`.
   - If the SHA did not change, stop and report a no-op.

3. Identify public API surface changes introduced by the bump.
   - Diff the old/new submodule SHAs and collect added or renamed `*.proto` files under:
     - `api/src/bitdrift/public/`
     - `api/src/bitdrift_public/`
   - Read the changed public API service protos, especially files like
     `api/src/bitdrift/public/**/v1/api.proto`, and note any newly imported/exported public API
     types.
   - Treat newly imported/exported protos as a signal that `bd-proto/src/proto_config.rs` or
     `bd-proto/src/protos/**/mod.rs` may need to change even if `cargo build` has not run yet.

4. Regenerate protobuf artifacts.
   - Run `cargo build -p bd-proto --features public-api`.
   - Run `cargo build -p bd-proto --features with-source-info`.
   - If either build fails for reasons unrelated to the API bump, stop and surface the failure.

5. Update `bd-proto/src/proto_config.rs` for newly exported public API protos.
   - Inspect `get_public_api_proto_configs()` and the nearest related output directory.
   - For each new public API proto:
     - If it belongs with the top-level shared public API outputs, add it to the existing
       `src/protos/public_api` config.
     - If it belongs with a service-specific output directory, extend the matching config instead of
       creating a parallel one.
     - Only add a new `ProtoConfig` entry when there is no clear existing home.
   - Keep edits minimal and consistent with nearby config entries.

6. Update `bd-proto` module wiring for new generated files.
   - Check `bd-proto/src/protos/public_api/mod.rs` and
     `bd-proto/src/protos/public_api_with_source/mod.rs` for newly generated top-level modules that
     need `pub mod <name>;`.
   - Check service subdirectory `mod.rs` files such as:
     - `bd-proto/src/protos/public_api/*/mod.rs`
     - `bd-proto/src/protos/public_api_with_source/*/mod.rs`
   - If a generated service `api.rs` references `super::<name>`, make sure the containing `mod.rs`
     exposes that dependency, usually by re-exporting it from the parent module with
     `pub use super::{...};`.
   - When unsure, follow the existing pattern used by neighboring modules in the same directory.

7. Re-run generation after config or module edits.
   - Run `cargo build -p bd-proto --features public-api` again.
   - Run `cargo build -p bd-proto --features with-source-info` again.
   - Stop if either path still fails and report the concrete error.

8. Inspect and scope the diff before committing.
   - Inspect with `git --no-pager status --short` and `git --no-pager diff --stat`.
   - Confirm changes are limited to:
     - the `api` submodule pointer,
     - generated `bd-proto` outputs,
     - intended `proto_config.rs` edits,
     - intended `mod.rs` wiring edits,
     - any directly related documentation or tests.
   - If unrelated files changed, stop and ask how to proceed.

9. Create a branch, commit, push, and open a PR if the user asked for one.
   - Prefer GitHub MCP for PR creation when available; otherwise use non-interactive git and `gh`.
   - Branch name: `chore/update-api-<new-sha-short>`.
   - Build commit metadata from concrete values already printed in output, not shell variables.
   - Commit title: `Update api and regenerate protos`
   - Commit body should summarize:
     - the submodule bump,
     - generated proto updates,
     - any `proto_config.rs` and `mod.rs` wiring added for newly exported public API protos.
   - Do not include a generic Validation section in the PR body.

## Safety rules

- Never hide build errors or git failures.
- Never claim the update is complete before both `cargo build -p bd-proto --features public-api`
  and `cargo build -p bd-proto --features with-source-info` succeed.
- Do not include unrelated changes in the commit.
- Prefer extending existing config and module patterns over inventing new layout or naming schemes.
