# monedula-sim-link

[![CI](https://github.com/monedula-dev/monedula-sim-link/actions/workflows/ci.yml/badge.svg)](https://github.com/monedula-dev/monedula-sim-link/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/monedula-dev/monedula-sim-link)](https://github.com/monedula-dev/monedula-sim-link/releases)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A read-only Go CLI that turns a real Kafka cluster into a shareable [Kafka Simulator](https://monedula.dev/kafka-simulator/) link, with an MCP server for AI agents.

**Documentation:** [monedula.dev/flock/docs/sim-link](https://monedula.dev/flock/docs/sim-link/)

- [See your cluster in the simulator](https://monedula.dev/flock/docs/sim-link/tutorials/see-your-cluster-in-the-simulator/): from install to your first link
- [The read-only guarantee](https://monedula.dev/flock/docs/sim-link/concepts/read-only-guarantee/): every request the tool sends, and the tests that prove none of them writes
- [`connect` reference](https://monedula.dev/flock/docs/sim-link/reference/cli/connect/): every flag and exit code
- [Use the MCP server](https://monedula.dev/flock/docs/sim-link/how-to/use-the-mcp-server/) and its [tools](https://monedula.dev/flock/docs/sim-link/reference/mcp-tools/)

## Install

```bash
go install github.com/monedula-dev/monedula-sim-link/cmd/monedula-sim-link@latest
```

Prebuilt binaries for Linux, macOS and Windows are on the [releases page](https://github.com/monedula-dev/monedula-sim-link/releases).

## Try it

Snapshot two topics of a local cluster and print the playground link that shows them:

```bash
monedula-sim-link connect --brokers localhost:9092 --topics orders,payments
```

Add `--dry-run` to see the cluster shape, the real-to-simulator name tables and the decoded actions before the URL. [Connect to a secured cluster](https://monedula.dev/flock/docs/sim-link/how-to/connect-to-a-secured-cluster/) covers TLS, mTLS and SASL (PLAIN and SCRAM).

## Read-only

`connect` sends only read requests: ApiVersions, Metadata, ListOffsets, ListGroups, DescribeGroups, OffsetFetch and DescribeConfigs. Metadata goes out with `AllowAutoTopicCreation=false`, so looking up a topic never creates it. It joins no consumer group and commits no offset. The whole client is in [`snapshot/kafka.go`](snapshot/kafka.go), so the guarantee can be audited in one file.

## MCP server

`monedula-sim-link mcp` serves the Model Context Protocol over stdio, so Claude Code, Claude Desktop or another MCP client can build links: scenario deep links, free-play sessions from a description, and a live cluster snapshotted read-only. In Claude Code:

```bash
claude mcp add monedula-sim-link -- monedula-sim-link mcp
```

## Building and testing

With Go 1.25 or newer:

```bash
git clone https://github.com/monedula-dev/monedula-sim-link.git
cd monedula-sim-link
go build ./cmd/monedula-sim-link
go test ./...
```

`go test ./...` needs no Kafka: the integration tests run against an in-process fake cluster (franz-go's `kfake`). [`e2e/`](e2e/) runs the same pipeline against a real three-node KRaft cluster in Docker with `e2e/run.sh`, and checks that nothing on the cluster changed. CI runs it against Apache Kafka 4.3.1 and 3.9.2, Confluent Platform 8.3.2 and 7.9.10, and Confluent Server 8.3.2.

## Contributing

Report bugs and request features in [GitHub issues](https://github.com/monedula-dev/monedula-sim-link/issues).

## License

monedula-sim-link is licensed under the Apache License 2.0; see [LICENSE](LICENSE).

Apache Kafka, Kafka and the Kafka logo are trademarks of The Apache Software Foundation. monedula-sim-link is an independent project that works with Kafka clusters; it is not affiliated with, endorsed by or sponsored by the ASF.
