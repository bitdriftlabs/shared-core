# Logger CLI

The logger CLI is a wrapper around the core logging framework that allows instantiating a logger
that can be used to test parts of the logger that does not depend on platform specific code.

## Usage

The CLI operates in a client/server architecture to allow simulating persistent logging sessions.

First install the `logger-cli` for ease of use:

```bash
cargo install --path logger-cli
````

To start the CLI, run

```bash
logger-cli start
````

providing both API_URL and API_KEY, .e.g.

```bash
API_KEY=<key> API_URL=<url> logger-cli start
logger-cli --api-key <key> --api-url <url> start
```

See `logger-cli start --help` to see additional options that can be used when starting the logger.

This will start the logger that will run until stopped via ^C.

To interact with the running logger, execute other logging commands from another terminal:

```bash
logger-cli log <message>
logger-cli log <message> --field <key1> <value1> --field <key2> <value2>
```

See `logger-cli --help` for a complete list of commands that be run.

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

