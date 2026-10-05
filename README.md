# monedula-sim-link

A Go CLI that connects **read-only** to a real Kafka cluster and generates a
[monedula.dev kafka-simulator playground](https://monedula.dev/kafka-simulator/playground)
URL that visualizes it - brokers, topics, partition placement, failures, a
sample of data volume, consumer groups and topic configs - as a shareable
link.

```bash
$ monedula-sim-link connect --brokers localhost:9092 --topics orders,payments
https://monedula.dev/kafka-simulator/playground?scenario=free&actions=…&size=3x6x3x…
```

It also ships an [MCP server](#mcp-server), so an AI agent can build the same
links, and it is a reference generator for the
[playground URL format](https://monedula.dev/flock/docs/kafka-simulator/reference/url-format/).

## Install

```bash
go install github.com/monedula-dev/monedula-sim-link/cmd/monedula-sim-link@latest
```

or download a prebuilt binary for Linux, macOS or Windows from
[Releases](https://github.com/monedula-dev/monedula-sim-link/releases). Building from
source needs Go 1.25 or newer.

## Read-only guarantee

`connect` **never writes to your cluster**. The only Kafka requests the tool
issues are **ApiVersions** (the franz-go handshake), **Metadata**,
**ListOffsets**, **ListGroups**, **DescribeGroups**, **OffsetFetch** and
**DescribeConfigs** - every one a read request. The Metadata request is sent
with `AllowAutoTopicCreation=false`, so looking up a topic can never create it.
No records are produced, no topics or configs are created or altered, no
consumer group is joined (no JoinGroup/SyncGroup/Heartbeat), no offset is
committed (no OffsetCommit). The entire client surface lives in
[`snapshot/kafka.go`](snapshot/kafka.go), where the guarantee is auditable in
one file; tests prove a failed topic lookup does not auto-create
(`TestFetchMissingTopicDoesNotAutoCreate`) and that reading a group's
committed offsets twice in a row never changes them (`TestFetchGroups`, both
in `snapshot/kafka_test.go`).

## Usage

```bash
# Plaintext local cluster, all non-internal topics:
monedula-sim-link connect --brokers localhost:9092

# Pick topics: exact names, a regex, or both. Exact names are also the only
# way to include internal topics (flagged internal by the broker, or with a
# leading `_`):
monedula-sim-link connect --brokers localhost:9092 --topics orders,payments
monedula-sim-link connect --brokers localhost:9092 --topics-regex '^prod-'
monedula-sim-link connect --brokers localhost:9092 --topics __consumer_offsets

# TLS + SCRAM-SHA-512 (typical managed cluster). Prefer the env var for the
# password so it stays out of shell history:
export MONEDULA_SIM_LINK_PASSWORD='…'
monedula-sim-link connect \
  --brokers broker1.example.com:9093,broker2.example.com:9093 \
  --tls --ca-cert ./cluster-ca.pem \
  --sasl-mechanism scram-sha-512 --username viewer

# mTLS:
monedula-sim-link connect --brokers broker:9093 \
  --tls --ca-cert ca.pem --client-cert client.pem --client-key client.key

# Dry run: print the size param, the real→simulator broker/topic name tables
# and the decoded action list before the URL:
monedula-sim-link connect --brokers localhost:9092 --dry-run

# Selection larger than the simulator renders (10 brokers / 6 extra topics /
# 24 partitions per topic / 4 consumer groups)? The default is a hard failure
# listing every broker, topic and partition violation at once (the
# consumer-group cap is checked once those pass); --clamp trims
# deterministically (lowest ids / alphabetical) with warnings instead:
monedula-sim-link connect --brokers localhost:9092 --clamp

# No cluster handy: feed a built-in fixture (offline broker, custom placement,
# ISR gap, data) through the same pipeline:
monedula-sim-link demo --dry-run

# Point at a local playground dev server instead of production:
monedula-sim-link connect --brokers localhost:9092 \
  --base-url http://localhost:4321/kafka-simulator/playground
```

### `connect` flags

| Flag | Meaning |
|------|---------|
| `--brokers` | Comma-separated bootstrap brokers (`host:port`). Required. |
| `--topics` | Comma-separated exact topic names. Fails if any is missing. Only way to include internal topics (flagged internal by the broker, or with a leading `_`). |
| `--topics-regex` | Additionally include non-internal topics matching this Go regexp. |
| `--tls` | Connect over TLS (implied by the cert flags). |
| `--ca-cert` | PEM CA file appended to the system trust pool. |
| `--client-cert` / `--client-key` | PEM client keypair for mTLS. |
| `--sasl-mechanism` | `plain`, `scram-sha-256` or `scram-sha-512`. |
| `--username` / `--password` | SASL credentials; prefer `MONEDULA_SIM_LINK_PASSWORD` over `--password`. |
| `--timeout` | Overall snapshot timeout (default 15s). |
| `--clamp` | Trim over-cap selections deterministically instead of failing. |
| `--max-records-per-partition` | Synthetic trailing records per partition (default 8). |
| `--base-url` | Playground base URL. |
| `--dry-run` | Also print the decoded action list, name mappings and size param. |
| `--force` | Emit the URL even when it exceeds the length limit (over-length links risk truncation). |

If the topic selection matches nothing, the tool fails with a clear message -
it never emits an empty visualization silently.

## What the link shows

The mapping is fully documented in **[docs/mapping.md](docs/mapping.md)** -
including every approximation and what is knowingly *not* represented (exact
real offsets, controller/KRaft state, rack topology). In short:

- **Cluster shape** via the free-play `size` param: broker count, the selected
  topics with their real partition counts and RF.
- **Replica placement** via `reassign_partition` actions - only for partitions
  whose real assignment differs from the simulator's default placement (a
  byte-exact port of the simulator's Apache-Kafka placement algorithm decides).
- **Failures**: brokers that are offline or missing from metadata are killed
  (`K:`); brokers assigned but out of an ISR are slowed (`SR:`) so the
  simulator shows them lagging out of the ISR too.
- **Data volume**: up to `--max-records-per-partition` synthetic keyed records
  per partition, with keys brute-forced through the simulator's own
  partitioner hash so each record lands on its intended partition.
- **Consumer groups**: each real group with a committed offset on a selected
  topic becomes an `add_group` with its (capped) real members joined via
  `add_consumer`; a group with measurable lag gets `auto.offset.reset` set to
  `earliest` so it visibly drains the topic's synthetic backlog instead of
  starting caught up (exact offsets are not carried - see docs/mapping.md).
- **Topic configs**: `cleanup.policy`, `min.insync.replicas`, `retention.ms`
  and `retention.bytes` from `DescribeConfigs` land on the extra topics' `size`
  segment and, for the primary topic, its config tail segment (§11.3/§11.4 of
  the URL spec).
- **Diskless topics** (v2.0+, KIP-1163 draft): the codec can encode an extra
  topic's `disklessEnable` flag and the primary topic's diskless flag + timing
  override (§11.3/§11.4 of the URL spec), but this mapping never sets either from a
  live snapshot - `diskless.enable` has no `DescribeConfigs` equivalent on a
  real cluster today. Encoding support exists purely for parity with the
  simulator's own URL codec.

## Self-validation before printing

The playground decoder **drops malformed entries silently**, so the tool
refuses to print a link it cannot prove safe:

1. the action log must round-trip `encode → decode → encode` losslessly
   (never overridable);
2. the final URL must stay under 2048 chars (longer links get truncated by
   browsers/chat clients) - `--force` downgrades to a warning.

The plain representation is still preferred (human-readable, most portable), but
the **compressed `~1` form is no longer refused**: the playground now decodes
both the raw-DEFLATE layout this tool emits and its own lz-string layout (see the
compatibility note below).

## MCP server

`monedula-sim-link mcp` serves the **Model Context Protocol over stdio**, so any MCP
client - Claude Code, Claude Desktop, or another LLM agent - can generate
playground links: curated scenario deep links, bespoke free-play sessions built
from a declarative description, and the visualization of a **live cluster**
snapshotted read-only (`cluster_to_url`). It uses the official
[`modelcontextprotocol/go-sdk`](https://github.com/modelcontextprotocol/go-sdk).

Every tool goes through the fail-fast codec: action fields are validated one
by one, `config_change` paths are checked against the §6 whitelist, the
`MAX_URL_ACTIONS` cap and the §12 free-play size clamps are enforced, and no
URL is returned unless its `actions` value self-decodes back to the identical
log. Validation errors name the offending field / separator / config path, so
a calling LLM can fix its input instead of shipping a link the playground
would silently mangle.

### Client registration

[Install](#install) the binary, then register it. The examples assume it is on your `PATH`; otherwise give its absolute path as `command`.

**Claude Code** - `.mcp.json` at the project root (or `claude mcp add
monedula-sim-link -- monedula-sim-link mcp`):

```json
{
  "mcpServers": {
    "monedula-sim-link": {
      "command": "monedula-sim-link",
      "args": ["mcp"]
    }
  }
}
```

**Claude Desktop** - `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "monedula-sim-link": {
      "command": "monedula-sim-link",
      "args": ["mcp", "--base-url", "https://monedula.dev/kafka-simulator/playground"]
    }
  }
}
```

`--base-url` is optional (shown with its default); point it at
`http://localhost:4321/kafka-simulator/playground` to generate links against a
local dev server.

### Tools

**`build_playground_url`** - a free-play session from a declarative
description: optional `cluster` topology (`single-dc`, `active-passive`,
`active-active`, `stretched-2-5`, `stretched-3`, `diskless-3az`), optional `shape`
(brokers, topics with partitions / RF / min.ISR / cleanup policy, rack-aware
flag, and - KIP-1163 draft, v2.0+ - a per-topic `disklessEnable`
flag plus a session-wide `disklessTiming` override) mapped onto the `cluster` +
`size` params (§9–§14), and an ordered list of timed `actions`. Returns the URL
plus an echo of the actions **decoded back out of it**, so the caller can
confirm losslessness.

`shape.disklessTiming` (`commitIntervalMs` / `uploadMs`, both optional
integers in `[100, 5000]`) requires a diskless topic (`disklessEnable`, or the
`diskless-3az` preset) - mirroring the simulator's own "kept only when some
topic is diskless" rule; the request is rejected otherwise instead of
silently dropping the override. Producer/consumer-group `client.rack` (also KIP-1163 draft, added on
the TypeScript side) has no equivalent here yet - this tool's declarative
shape does not model initial producers or consumer groups at all.

```json
{
  "shape": {
    "brokers": 6,
    "topics": [
      { "name": "orders", "partitions": 4, "replicationFactor": 3 },
      { "name": "payments", "partitions": 6, "replicationFactor": 3,
        "minInSyncReplicas": 2, "cleanupPolicy": "delete" }
    ]
  },
  "actions": [
    { "kind": "add_group", "at": 1000, "groupId": "analytics", "topics": ["orders"] },
    { "kind": "produce", "at": 2000, "producerId": "p1", "topic": "orders", "key": "O1" },
    { "kind": "config_change", "at": 3000, "path": "producers.p1.config.acks", "value": "all" },
    { "kind": "kill_broker", "at": 8500, "brokerId": "broker-2" },
    { "kind": "restart_broker", "at": 22000, "brokerId": "broker-2" }
  ]
}
```

→ `https://monedula.dev/kafka-simulator/playground?scenario=free&actions=…&size=6x4x3xpayments~6~3~2~d~~~~`
plus `decodedActions` echoing the five actions.

Action kinds: `produce` (key / value / tombstone), `kill_broker` (hard /
graceful), `restart_broker`, `add_broker`, `remove_broker`, `add_producer`, `remove_producer`,
`add_group` (optional `autoOffsetReset`), `add_consumer`, `add_share_group`, `config_change` (§6 whitelist,
incl. `clear` for the clearable knobs), `reassign_partition`,
`change_replication_factor`, `set_replica_speed`, `tier_offload`.

**`open_scenario`** - the paused scripted-walkthrough deep link for a curated
scenario id:

```json
{ "scenarioId": "00.1.3" }
```

→ `https://monedula.dev/kafka-simulator/playground?scenario=00.1.3`

**`validate_actions`** - what the playground would *actually* load from a raw
actions string (or a full playground URL - the `actions` param is extracted).
The playground drops unparseable entries silently; this tool reports them:

```json
{ "actions": "K:broker-2@8500,NOPE,AG:analytics:orders@1000" }
```

→ 2 surviving entries (sorted by `at`), `dropped: [{ "entry": "NOPE",
"reason": "missing @<at>" }]`, plain-vs-compressed, and the length / count
against the caps (1500-char compression threshold, 2000-action
`MAX_URL_ACTIONS`).

**`cluster_to_url`** - the live-cluster counterpart to `build_playground_url`:
connect **read-only** to a real Kafka cluster (the same
[`monedula-sim-link connect`](#read-only-guarantee) path - ApiVersions, Metadata,
ListOffsets, ListGroups, DescribeGroups, OffsetFetch and DescribeConfigs only)
and emit the URL that visualizes it. Inputs mirror the CLI: bootstrap
`brokers` (required), an exact `topics` list and/or a `topicsRegex`, TLS
(`caCert` / `clientCert` / `clientKey` are **file paths resolved on the MCP
server's host**, not the client's), SASL (`saslMechanism` / `username` /
`password` - prefer the `MONEDULA_SIM_LINK_PASSWORD` env var on the server host),
`timeoutSeconds`, `maxRecordsPerPartition` and `clamp`.

```json
{ "brokers": ["broker1:9092", "broker2:9092"], "topics": ["orders", "payments"] }
```

→ the URL, the `size` param (now carrying each topic's real `min.insync.replicas`
/ `cleanup.policy` / `retention.ms` / `retention.bytes` from `DescribeConfigs`),
the real→simulator `brokerNames` / `topicNames` / `groupNames` tables (a
consumer group with no committed offset on a rendered topic, or dropped by the
4-group cap, is absent from `groupNames`), any `warnings` (unreadable offsets,
approximations, clamps), and the actions **decoded back out of the URL** -
including `add_group` / `add_consumer` for every mapped consumer group and its
(capped) real members - so the caller can confirm the snapshot mapping is
lossless. A selection matching nothing is an actionable error; a selection
over the free-play caps fails unless `clamp` is set, listing every broker,
topic and partition violation at once (the 4-group cap is checked once those
pass); neither case emits a silent or wrong link.

## Architecture

```
                Kafka cluster (read-only)
                        │  franz-go: ApiVersions + Metadata + ListOffsets +
                        │  ListGroups + DescribeGroups + OffsetFetch +
                        │  DescribeConfigs only
                        ▼
   snapshot.ClusterSnapshot   (package snapshot)   ← plain boundary type + topic Selection
                        │
                        ▼
     mapping.Map(...)         (package mapping)    ← snapshot → size param + []actionlog.Entry
                        │                            (docs/mapping.md)
                        ▼
  actionlog.Encode / Verify   (package actionlog)  ← the plain action-log grammar (§2) + validation (§3/§7)
                        │
                        ▼
    simurl.Build(...)         (package simurl)     ← URL assembly, percent-encoding (§7.3), compression (§4)
                        │
                        ▼
             playground URL (printed)
```

### Packages

- **`actionlog`** - Go types + encoder/decoder for the plain action-log
  grammar (`kill_broker`, `add/remove_producer`, `produce_record`,
  `reassign_partition`, `set_replica_speed`, groups, configs, …). Fail-fast,
  the opposite of the tolerant playground decoder: invalid fields, reserved
  separators (`, : @ ~ . |`), non-whitelisted config paths and the
  2000-action cap are hard errors. `Verify` proves a log round-trips.
- **`simurl`** - assembles the playground URL; plain vs compressed choice,
  percent-encoding, built-in round-trip self-check.
- **`snapshot`** - the `ClusterSnapshot` boundary type, the topic `Selection`
  rules, and the **only** code that talks to Kafka (franz-go).
- **`mapping`** - snapshot → simulator state: the `size` param builder plus
  byte-exact ports of the simulator's replica-placement algorithm
  (`src/sim/placement.ts`) and keyed-partitioner hash
  (`src/sim/producer.ts`), both fixture-verified against the TypeScript
  output (`mapping/testdata/ts-fixtures.txt`).
- **`mcpserver`** - the MCP server: tool schemas, handlers and the
  shape → `cluster`/`size` mapping (§9–§14 generator-safe subset). The
  `cluster_to_url` tool reuses the `internal/emit` pipeline. Tested in-process
  over the SDK's in-memory transport pair (`kfake` behind `cluster_to_url`).
- **`internal/emit`** - the shared snapshot → mapping → self-validation → URL
  pipeline (`Snapshot`, `Build`) used by both the CLI and the `cluster_to_url`
  tool, so both emit byte-identical links from one read-only snapshot.
- **`cmd/monedula-sim-link`** - the CLI (`connect`, `demo`, `mcp`), a thin wrapper over
  `internal/emit`.

## Testing

`go test ./...` needs **no external Kafka**: integration tests spin an
in-process fake cluster with franz-go's
[`kfake`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake) (3 brokers,
seeded topics, records produced onto chosen partitions), snapshot it through
the real client path, and assert the generated URL's decoded action log and
size param match expectations end to end.

```bash
gofmt -l .        # must be empty
go vet ./...      # must be clean
go test ./...     # must be green
```

### End-to-end tests against real Kafka

`e2e/` runs the same pipeline against a real three-node KRaft cluster
(`e2e/compose.yaml`, one rack per node) instead of `kfake`. It seeds topics,
records and two consumer groups, then checks the snapshot, the generated URL,
the `connect` binary and the `mcp` server over stdio, and that nothing on the
cluster changed: no topic auto-created, no offset committed, no member joined.
A last test stops a node and checks the link shows it dead, including a
partition left with no leader. The tests carry the `e2e` build tag, so
`go test ./...` skips them. They need Docker:

```bash
e2e/run.sh                                          # apache/kafka:4.3.1
KAFKA_IMAGE=confluentinc/cp-kafka:8.3.2 e2e/run.sh
KEEP=1 e2e/run.sh -run TestReadOnly                 # keep the cluster up
```

CI runs the suite once per image:

| Image | Distribution |
| --- | --- |
| `apache/kafka:4.3.1`, `apache/kafka:3.9.2` | Apache Kafka |
| `confluentinc/cp-kafka:8.3.2`, `confluentinc/cp-kafka:7.9.10` | Confluent Platform, community broker |
| `confluentinc/cp-server:8.3.2` | Confluent Server, on its built-in trial license |

Requires Go ≥ 1.25. Third-party
dependencies: `github.com/twmb/franz-go` (+ `kmsg`, and `kfake` and `kadm`
for tests only) and the official MCP Go SDK
(`github.com/modelcontextprotocol/go-sdk`); the codec packages themselves
remain standard-library only.

## Spec reference

The grammar is defined by the public **[playground URL format](https://monedula.dev/flock/docs/kafka-simulator/reference/url-format/)** specification
(the free-play `size` param is specified in its §9–§14). This module implements it
directly:

- §2 plain grammar and §2.2 escaping (produce value `~`→`~t` then `,`→`~c`;
  config value `,`→`~`).
- §3 / §7 validation (reserved separators, `MAX_URL_ACTIONS`, config whitelist).
- §4 compression (`~1` + base64url(raw DEFLATE) with incompressible fallback;
  the decoder also accepts the playground's lz-string layout).
- §6 the `config_change` accepted-path whitelist.
- §7.3 always percent-encode the `actions` value.
- §8 worked examples are asserted byte-for-byte in the conformance tests.
- §12 the 4-segment `size` back-compat form the mapping emits.

### Compatibility note on the compressed form

The **plain** action log this tool emits is byte-compatible with the playground
and is the preferred, most-portable target.

The **compressed** form emits `~1` + `base64url(raw DEFLATE(plain))`, exactly as
`docs/playground-url-api.md` §4 specifies. The `~1` prefix covers **two** accepted
layouts and both decode in the playground *and* here:

- **raw DEFLATE + base64url** - what this tool emits (Go `compress/flate`).
- **lz-string base64url** - what the playground itself emits.

The playground decoder tries raw DEFLATE first and falls back to lz-string, so a
`~1` value produced here decodes there, and this tool's `validate_actions`
(via `simurl.PlainActions` / `decompress`) decodes a playground-generated
lz-string `~1` value too. Compression is therefore no longer refused; keep logs
plain and under the 1500-char threshold when practical purely for readability
and URL length.

## Follow-up scope

- **Exact consumer-group positions.** The action log has no primitive to seek
  a group to an arbitrary offset, so a group with lag is approximated by
  switching it to `auto.offset.reset=earliest` (draining the topic's synthetic
  backlog) rather than reproducing its real numeric committed offset. A
  `seek`/position-setting action would let this be exact.
- **Kafka Connect / KIP-932 share groups.** `fetchGroups` only reads groups
  whose `ListGroups` protocol type is `consumer`; Connect worker groups and
  share groups are skipped (the simulator's `add_share_group` targets a
  different, not-yet-wired KIP-932 source).

## License

[Apache License 2.0](LICENSE).

Apache Kafka, Kafka and the Kafka logo are trademarks of The Apache Software
Foundation. monedula-sim-link is an independent project that works with Kafka
clusters; it is not affiliated with, endorsed by or sponsored by the ASF.
