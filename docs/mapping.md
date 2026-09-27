# Snapshot → simulator mapping

How `mapping.Map` turns a `snapshot.ClusterSnapshot` (the read-only point-in-time
view of a real cluster) into a kafka-simulator **free-play state**: the `size`
query param describing the cluster shape plus an action log reproducing
placement, failures and data volume. The contract: everything the emitted URL
shows is derived deterministically from the snapshot — same snapshot, same URL.

The target is the **single-DC free-play topology** (the `cluster` param is
omitted). Spec references: `docs/playground-url-api.md` in the website repo
(§2 action grammar, §9–§14 size param); simulator sources cited per section.

## Pipeline and determinism

1. **Brokers** — the universe is the metadata brokers plus any broker id
   referenced by a replica/ISR/leader list but absent from metadata (a
   dead-but-assigned node). Ordered numerically when every id is an integer
   (the normal Kafka node-id case), lexicographically otherwise, then named
   `broker-1..broker-N` in that order.
2. **Topics** — the selected topics sorted by real name; a topic named exactly
   `orders` becomes the simulator's fixed primary topic and goes first, the
   rest are free-play *extra topics* in alphabetical order. Each topic's
   `DescribeConfigs` result (cleanup policy, min.ISR, retention) travels with it.
3. **Size param** — brokers/partitions/RF/config per the shape rules below.
4. **Action log** — in fixed order: remove the free-play template producer →
   reassignments → kills → replica slowdowns → per-topic produce bursts →
   consumer groups. Actions are spaced 250 ms apart on the simulation timeline
   starting at t=500 ms.

## Cluster shape: the `size` param

Emitted as `<brokers>x<partitions>x<rf>[x<extraTopics>[x…x<primaryConfig>[x…x<diskless>]]]`
(playground-url-api.md §11.3; the base describes the `orders` primary): tail
index 0 for extra topics (the documented 4-segment back-compat form), tail
index 10 for the primary topic's own min.ISR/cleanup/retention when it
carries a config override (§11.4), and - KIP-1163 draft, v2.0+ -
tail index 17 for the primary diskless flag + diskless timing when either is
set (§11.3/§11.4). Values are pre-clamped to the
simulator's own caps so the slug is a **fixed point** of the simulator's
`clampFreePlaySize` — what the URL says is exactly what renders. The size
fixtures in `mapping/size_test.go` were round-tripped through the real
TypeScript codec (`src/sim/freePlayTopologies.ts`) to prove that.

- **No real `orders` topic selected** → the base still describes the primary,
  as a minimal `1x1` placeholder (the primary cannot be removed via the URL).
  A warning says so.
- **Extra topics** encode as `name~parts~rf~minISR~cleanup~retMs~retBytes~~`:
  `min.insync.replicas` and `cleanup.policy` from the topic's real
  `DescribeConfigs`, clamped to `[1, rf]` / one of `delete`/`compact`/
  `compact,delete` (falling back to `1`/`delete` when unread); `retention.ms`
  and `retention.bytes` carried when positive (Kafka's own `-1` "disabled"
  sentinel is treated as unset, matching the simulator's absent-field
  default); remote-storage tiering is never fetched, so those two trailing
  fields stay empty.
- **Primary topic config** (tail index 10) carries the same four fields for
  `orders` when any of them is known — omitted entirely when none is.
- **Diskless (v2.0+, KIP-1163 draft)**: an extra topic's `disklessEnable`
  appends a 10th `~1` field; the primary diskless flag + timing (tail
  index 17) encodes via the exported `mapping.EncodeDisklessTail` (backed by
  the unexported `disklessTail` type) in `mapping/size.go`. Real Kafka
  has no `diskless.enable` `DescribeConfigs` equivalent today, so `Map` never
  sets either - `buildSize` always receives an empty `disklessTail{}` - but
  the encoding is byte-identical to the simulator's own codec when a caller
  does supply one (see `mapping/size_test.go` and the diskless `SIZE` lines in
  `mapping/testdata/ts-fixtures.txt`). `mcpserver.ClusterShape.DisklessTiming`
  (the declarative-shape MCP tool) calls the same exported helper, so the two
  Go entry points into this tail segment cannot drift apart.
- **RF** per topic is the widest replica list observed across its partitions,
  clamped to the rendered broker count.
- A literal `x` in a topic name is slug-escaped to `*` (the simulator's
  `escapeNameForSlug`); action-log entries carry the name unescaped.

### Caps and `--clamp`

Simulator rendering caps (constants in `src/sim/freePlayTopologies.ts`,
documented in playground-url-api.md §13):

| Cap | Value |
|-----|-------|
| Brokers (single-DC) | 10 |
| Partitions per topic | 24 |
| Extra topics besides `orders` | 6 |

A selection over any cap **fails with a message listing every violation** by
default. With `--clamp` it is trimmed **deterministically** instead, with a
warning: brokers keep the N lowest ids, topics keep the alphabetically first 6
extras, partitions keep ids `0..23`. Replicas pointing at clamped-away brokers
are dropped from reassignments (warned per partition).

## Topic and broker naming

Simulator broker ids are positional (`broker-1..broker-N` over the
deterministic broker order); the real id → sim id table is printed by
`--dry-run`.

Topic names must survive two grammars: the simulator's own sanitizer (keeps
`[A-Za-z0-9._-]`, 40 chars) *and* the action-log field rules, which reserve `.`
as a list separator. So a real topic name is mapped by replacing every
character outside `[A-Za-z0-9_-]` with `-` (e.g. `billing.events` →
`billing-events`), truncating to 40. Collisions after sanitization get a
deterministic `-2`, `-3`… suffix. Every rename is warned and listed by
`--dry-run`.

## Replica placement: `RA` reassignments

The simulator places every free-play topic with its **default placement**: the
audited port of Apache Kafka's `assignReplicasToBrokers` over a single
synthetic rack (`assignRackAware` in `src/sim/placement.ts`, used with rack
`'single'` — the `faithfulPlacement` path). `mapping/placement.go` is a
byte-exact Go port, verified against the TypeScript output by fixture tests
(`mapping/testdata/ts-fixtures.txt`) — including the UTF-16 string sort that
orders `broker-10` before `broker-2`.

For each partition the mapping computes that default and compares it with the
real replica assignment **as a set**. Only partitions that differ get a
`RA:<topic>:<p>:<brokers>` action (real preference order preserved); partitions
already on default placement emit nothing, keeping URLs short.

Approximation: replica *order* (preferred leader) is not enforced, and the
simulator elects its own leader — usually the first surviving in-sync replica,
not necessarily the real leader.

## Failures: `K` kills and `SR` slowdowns

- A broker that is **offline** (in metadata as offline, or referenced by
  replica lists but absent from metadata) is added to the shape and then
  hard-killed (`K:broker-N`), so the canvas shows the real dead node and the
  simulator re-elects leaders away from it just like the real cluster did.
- An **under-replicated partition** (ISR ⊂ replicas) is approximated by
  `SR:broker-N:1` — slowing the lagging broker's follower replication so the
  simulator's ISR-shrink logic pushes it out of the ISR too. The approximation
  is **broker-wide**: the simulator throttles all of that broker's followers,
  while the real broker may lag on only some partitions. Each such broker is
  slowed once and warned once.

## Data volume: `P` produce bursts

Real record *contents* are never read (the tool speaks only Metadata and
ListOffsets), so data volume is shown with **synthetic trailing records**: per
partition, `min(latest - earliest, --max-records-per-partition)` records
(default 8) — relative fill, not absolute offsets.

To land each record on its intended partition, the mapping ports the
simulator's default keyed partitioner — **`partitionForKey` in
`src/sim/producer.ts`** (a cyrb53-derived 32-bit hash mod partition count; Go
port in `mapping/hash.go`, fixture-verified) — and **brute-forces short keys**
(`a`…`z9`, then 2–4 chars) until every partition has enough distinct keys that
hash to it. Keys are alphanumeric, so they are always action-log safe.

Per topic, in emission order: `AP:kpN:<topic>` adds a transient producer pinned
to the topic, the keyed `P:` records follow (partitions ascending, 250 ms
apart, ascending synthetic timestamps), and `RP:kpN` removes the producer 2 s
after the last record (past the produce flush delay). At most one synthetic
producer is alive at a time — free play caps producers at 4 — and the template
producer `p1` was removed at t=500 so the playground's auto-produce loop adds
nothing on top.

A partition whose rendered replicas are all dead gets **no** produces (they
could never acknowledge); a warning explains the skip.

## Consumer groups: `AG`/`AC` + the lag approximation

`fetchGroups` (`snapshot/kafka.go`) lists every group (`ListGroups`, filtered
to `ProtocolType == "consumer"` — Kafka Connect workers and KIP-932 share
groups are skipped), describes up to `MaxGroupsFetched` of them alphabetically
(`DescribeGroups`, for state and members) and fetches their committed offsets
restricted to the selected topics (`OffsetFetch`). All four requests are
read-only; none joins a group or commits an offset.

The mapping then, per real group (alphabetically first `MaxGroups` = 4 kept,
mirroring `MAX_FREE_PLAY_GROUPS` — a self-imposed canvas-legibility cap, not
an engine limit):

1. Intersects the group's committed-offset topics with the rendered topic
   set. **No overlap → the group is skipped** with a warning (the "committed
   offsets on a topic not selected" degrade-gracefully case).
2. Emits `AG:<simGroup>:<topic1>~<topic2>…` (`add_group`) for the surviving
   topics.
3. Sums `max(0, latestOffset - committedOffset)` over every partition the
   group has committed on. If that aggregate lag is `> 0`, emits
   `C:group.<simGroup>.consumer.autoOffsetReset:earliest` right after
   `add_group` — a real fresh group defaults to `latest` (starts at the high
   watermark), so this is the one lever the action-log grammar has to make a
   group visibly behind instead of caught up.
4. Joins up to `MaxGroupMembers` (= 4, mirroring `MAX_FREE_PLAY_GROUP_MEMBERS`)
   real members via `AC:<simGroup>:<simMember>` (`add_consumer`), named from
   each member's `ClientID` (falling back to its member id, then a positional
   `mN`) and deduped **by position**, not by name — two real members sharing
   one `ClientID` still get two distinct, non-colliding `add_consumer` entries.

**Approximation, not exact replay.** The action-log grammar has no
seek/commit-offset primitive, so a lagging group's *exact* committed offset is
never reproduced — only the fact that it has backlog, via the earliest reset.
Concretely: `add_group` defaults to the high watermark (Kafka's own `latest`
default), so a lagging group instead starts at `logStartOffset` and drains
forward through the topic's synthetic trailing records (see *Data volume*
above) — the visual effect is "this group is behind", not a byte-accurate lag
count.

## Self-validation

`Map` verifies its own output round-trips the codec
(`encode → decode → encode`), and the CLI re-decodes the final URL value,
refuses the deflate-compressed `~1` form (today's playground decodes lz-string;
see the README compatibility note) and refuses URLs over 2048 chars — `--force`
downgrades the last two to warnings. Losslessness is never overridable.

## What is NOT represented

- **Exact real offsets.** Only relative fill (up to the per-partition cap) is
  shown; the simulator's offsets start at 0. Earliest offsets (log truncation)
  are not modeled either — `earliest` only reduces the shown record count.
- **Record keys/values/timestamps.** Records are synthetic: brute-forced keys,
  no values, simulation-clock timestamps.
- **Exact consumer-group committed offsets.** See the lag approximation above
  — a group is shown as caught up or behind, not at its precise real offset.
- **Kafka Connect worker groups and KIP-932 share groups.** `fetchGroups` only
  reads `ProtocolType == "consumer"` groups.
- **Groups/members beyond the caps.** At most `MaxGroups` (4) real groups and
  `MaxGroupMembers` (4) real members per group are rendered, alphabetically
  first — the same canvas-legibility caps the free-play UI itself uses.
- **Controller / KRaft state.** The simulator boots its own 3-voter quorum;
  the real controller id and quorum layout are ignored.
- **Rack topology.** The single-DC free-play shape has no rack axis; real
  broker racks are captured in the snapshot but not encoded. (The simulator's
  rack model is a size-param extension the mapping does not emit.)
- **Per-client `client.rack`** (v2.0+, KIP-1163 draft). The TypeScript codec
  now encodes an optional rack on each initial producer / consumer group
  (`freePlayTopologies.ts`), but neither `Map` nor the MCP declarative shape
  (`mcpserver.ClusterShape`) models initial producers or consumer groups at
  all - there is nothing in this tool to carry a rack on, so this stays
  unrepresented until that surface exists.
- **Remote-storage tiering config.** `remote.storage.enable` and
  `local.retention.ms` are never fetched, even though `min.insync.replicas` /
  `cleanup.policy` / `retention.ms` / `retention.bytes` now are.
- **Diskless topics (v2.0+, KIP-1163 draft).** `diskless.enable` is never
  fetched either - it has no real `DescribeConfigs` equivalent today. The
  codec can encode it (see "Cluster shape" above), but `Map` never sets it.
- **Real leadership.** See the placement approximation note.
- **Live traffic.** The snapshot is a point in time; the URL replays a static
  reconstruction, not a stream.
