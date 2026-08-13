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

