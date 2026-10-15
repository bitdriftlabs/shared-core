# Logger CLI

The logger CLI is a wrapper around the core logging framework that allows instantiating a logger
that can be used to test parts of the logger that do not depend on platform-specific code.

## Usage

The CLI operates in a client/server architecture to allow simulating persistent logging sessions.

### Cargo

From `shared-core`, install the CLI for use as `logger-cli`:

```bash
cargo install --path logger-cli
```

Start the CLI, providing both `API_URL` and `API_KEY`:

```bash
API_KEY=<key> API_URL=<url> logger-cli start
logger-cli --api-key <key> --api-url <url> start
```

From another terminal, interact with the running logger:

```bash
logger-cli log <message>
logger-cli log <message> --field <key1> <value1> --field <key2> <value2>
```

### Bazel

From the monorepo root, build the CLI:

```bash
./bazelw build //shared-core/logger-cli:logger-cli-bin
```

Run it through Bazel, providing both `API_URL` and `API_KEY`:

```bash
API_KEY=<key> API_URL=<url> ./bazelw run //shared-core/logger-cli:logger-cli-bin -- start
./bazelw run //shared-core/logger-cli:logger-cli-bin -- --api-key <key> --api-url <url> start
```

From another terminal, interact with the running logger:

```bash
./bazelw run //shared-core/logger-cli:logger-cli-bin -- log <message>
./bazelw run //shared-core/logger-cli:logger-cli-bin -- log <message> --field <key1> <value1> --field <key2> <value2>
```

Pass CLI arguments after `--`. Run `logger-cli --help` or
`./bazelw run //shared-core/logger-cli:logger-cli-bin -- --help` for the complete command list.
Run `logger-cli start --help` or
`./bazelw run //shared-core/logger-cli:logger-cli-bin -- start --help` for start options.

The `start` command runs until stopped with `Ctrl-C`.

Logger CLI always fulfills remote screenshot device commands with a deterministic 1x1 JPEG. This
makes it suitable as the simulated device for end-to-end screenshot testing: after the command
reaches `Succeeded`, its attachment is available through the device-command attachment endpoint
as `image/jpeg`.

### Fake Perfetto command

Logger CLI always registers the application command `fake_perfetto`. It takes no arguments and
returns a deterministic 103-byte Perfetto protobuf trace as
`application/vnd.bitdrift.perfetto-trace`. The trace contains a `logger-cli` track and one
`fake-perfetto` instant event, plus one supported `PerfSample` referencing an interned,
already-symbolized `fake_perfetto` frame. This exercises profile processing and bundle publication
without requiring debug files. It is not a real CPU profile or a symbolication/name-recovery fixture;
the resulting bundle has empty symbolication sidecars.
The configured `--app-version-code` is included in artifact metadata as well as log fields.

Register the same key in the backend command catalog for the chosen app/platform, then select it
in a workflow command step. A workflow invocation has no direct command ID. Attach a session-flush
action to successful command completion so the uploaded `Command completed` log references the
attachment and drives its upload. Calling `upload-artifacts`, `--capture-session`, or a direct
command invocation does not verify that workflow path.

Use a fresh `--sdk-directory` and an unused `--port` for an isolated run. For Rust diagnostics:

```bash
RUST_LOG=info,bd_logger=debug,bd_workflows=trace,bd_api=debug \
  API_KEY=<key> API_URL=<url> logger-cli --sdk-directory .tmp/workflow-sdk --port 5502 \
  start --platform android --app-id io.bitdrift.cli.workflow
```

Deploy the workflow and confirm configuration receipt before sending a finite trigger sequence.
Send the trigger followed by one ordinary follow-up log: the command step is evaluated on the next
SDK event after the preceding match. Then wait for completion rather than repeatedly sending logs.
Verify both the uploaded completion log's typed attachment metadata and the exact raw attachment
bytes at ingestion; a handler log alone does not establish either upload.

Use exclusive execution for a single-run command test. Parallel execution with `maxActiveRuns: 1`
can evict the in-flight run when the SDK creates its next initial waiting run; its eventual command
completion is then discarded. For parallel tests, allow capacity for that initial run as well.

## MCP Server

An alternative way to interact with the logger CLI is via a small MCP server that allows an LLM agent to understand how to interact with the CLI.

To set up, build the mcp server using:
```bash

cargo install --bin logger-mcp -p logger-cli
```

And add the MCP server to the configuration for your agent of choice. For example, using opencode this looks like

```json
    "logger-cli": {
      "type": "local",
      "enabled": true,
      "command": ["logger-mcp"],
    },
```

At the moment logger-cli must be manually started via the `logger-cli` start above in order to work as the MCP server requires a running logger to interact with.

Currently only a handful of operations are supported but can be extended as the tool sees more use.
