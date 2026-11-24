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
   rest are free-play *extra topics* in alphabetical order. Over the 6-extra
   cap, the most eventful are kept (see *Caps* below). Each topic's
   `DescribeConfigs` result (cleanup policy, min.ISR, retention) travels with it.
3. **Size param** — brokers/partitions/RF/config per the shape rules below.
4. **Action log** - in fixed order: remove the free-play template producer and
   template consumer group → reassignments → kills → replica slowdowns →
   per-topic produce bursts → consumer groups → producers (only with
   `--producers`). Actions are spaced 250 ms apart on the simulation timeline
   starting at t=500 ms.

## Cluster shape: the `size` param

Emitted as `<brokers>x<partitions>x<rf>[x<extraTopics>[x…x<primaryConfig>[x…x<diskless>]]]`
(playground-url-api.md §11.3; the base describes the `orders` primary): tail
index 0 for extra topics (the documented 4-segment back-compat form), tail
index 10 for the primary topic's own min.ISR/cleanup/retention when it
carries a config override (§11.4), and - KIP-1163 draft, v2.0+ -
tail index 17 for the primary diskless flag + diskless timing when either is
set (§11.3/§11.4). Values are pre-clamped to the
simulator's own caps and written in the simulator's own canonical form, so the
slug is a **fixed point** of its codec: decoding it with `decodeFreePlaySize`
and encoding the result with `encodeFreePlaySize` gives the same string back.
What the URL says is exactly what renders, and the playground never rewrites
the link when it loads it. The `SIZE` lines in `mapping/testdata/ts-fixtures.txt`
are that round trip, run through the real TypeScript codec
(`src/sim/freePlayTopologies.ts`), and `TestBuildSizeIsFixedPointOfTypeScript`
in `mapping/size_test.go` replays every one of them against `buildSize`.

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
  `orders`, but only those that differ from the simulator's defaults, because
  the simulator drops a field equal to its default when it decodes the slug
  (`clampPrimaryTopic`). Cleanup `delete` is left out. `min.insync.replicas` is
  left out when it equals the default for the primary's RF, `max(1, RF-1)` (so 2
  at RF 3, and 1 at RF 1 and RF 2). Retention is carried when set. When nothing
  is left, tail index 10 is omitted entirely and the slug can end at the base:
  an `orders` on its defaults at 3 partitions and RF 3 is just `3x3x3`. A
  min.ISR of 1 at RF 3 with cleanup `delete` and a 7-day retention is
  `...xxxxxxxxxx1~~604800000~~`, not `...1~d~604800000~~`. The MCP
  `build_playground_url` shape tool writes the same segment through the same
  helper (`mapping.EncodePrimaryConfigTail`), so `minInSyncReplicas: 2` at RF 3 or
  `cleanupPolicy: delete` on `orders` leaves the slug at `6x4x3`. In an extra
  topic's chunk a min.ISR and a cleanup are always written; the shape tool writes
  Kafka's default (min.ISR 1, `delete`) for one it leaves unset, because the
  simulator drops a whole extra topic whose min.ISR field is not a number.
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
- **Schema Registry (tail index 14)**: written only by the MCP declarative
  shape (`mcpserver.ClusterShape.SchemaRegistry`); `Map` never sets it, since
  `connect` does not read a real registry. `{}` is `1`, the abstract registry
  (subjects and compatibility, no instances). v2.2 preview: `instancesPerDc`
  1-3 writes `2`-`4`, and `followerOnlyDcs` appends `r` and the sorted DC
  indexes (zone order), e.g. `3r1` = 2 instances per DC, the second DC
  follower-only. The slug is the 15-segment tail when nothing later is set,
  e.g. `1-1-1x3x3xxxxxxxxxxxxxxx3r1` on stretched-3. Out-of-range counts or
  indexes are refused rather than clamped. The `SIZE-SR` lines in
  `mapping/testdata/ts-fixtures.txt` pin both the slugs and the instance names
  the simulator builds from them.
- **Streamhouse sink (tail index 18; v3.0, `next`)**: written only by the MCP
  declarative shape on `cluster: "streamhouse"` (`ClusterShape.ConnectSink`),
  the 19-segment tail. See *Streamhouse* below.
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
| Consumer groups | 4 |

Brokers and partitions over their cap **fail** by default: both are checked
together and the failure lists every violation at once. With `--clamp` they
are trimmed **deterministically** instead, with a warning: brokers keep the N
lowest ids, partitions keep ids `0..23`. Replicas pointing at clamped-away
brokers are dropped from reassignments (warned per partition).

Topics and consumer groups over their cap do not fail. The mapping keeps the
most eventful ones, and one warning names what it kept, why, and what it
dropped (`mapping/rank.go`):

- **Topics.** Topics named exactly with `--topics` are kept first. The free
  slots go to the other selected topics, ranked by: partitions under-replicated
  or offline (no live leader, or an ISR smaller than the replica set), then
  committed-offset lag summed over every group, then how many groups have
  committed offsets on the topic, then retained records (latest minus earliest
  offset), then name. The kept topics are still emitted alphabetically. More
  than 6 extra topics in `--topics` itself is a cap breach: it fails without
  `--clamp`, and with it the most eventful listed topics are kept.
- **Consumer groups.** Only groups with a committed offset on a rendered topic
  count. Over 4, the groups with the most lag across the rendered partitions
  are kept, then the ones with the most members, then by id; `add_group`
  entries stay in id order.

Members per group (4) are not a failing cap: the first 4 are always kept, with
a warning, with or without `--clamp`.

The caps above are the single-DC ones this mapping targets. The MCP
`build_playground_url` shape tool also validates the multi-DC topologies, and
`stretched-3` has no constant partition cap: it follows the simulator's
`maxFreePlayPartitions`, the largest count from 1 to 6 that keeps the busiest
broker within 3 partition strips at the chosen RF (partition `i` sits on brokers
`i..i+rf-1` modulo the broker count, so after `p` partitions a broker holds at
most `rf*floor(p/brokers) + min(rf, p mod brokers)` of them). It never exceeds 6:
9 brokers at RF 3 allow 6 partitions and reject 7, 9 brokers at RF 4 allow 3.
`mcpserver/shape.go` ports the loop and `mcpserver/shape_test.go` pins its
answers to the simulator's own function.

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

Real record *contents* are never read (the tool never issues a Fetch; offset
ranges come from ListOffsets), so data volume is shown with **synthetic
trailing records**: per partition,
`min(latest - earliest, --max-records-per-partition)` records
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
nothing on top. (Without `--producers` that stays true for the whole session.
With it, the auto-loop has the producers it adds to run, see *Producers* below.)

A partition whose rendered replicas are all dead gets **no** produces (they
could never acknowledge); a warning explains the skip.

## Consumer groups: `AG`/`AC` + the lag approximation

**The template group goes first.** The single-DC free-play template starts with
a default consumer group `c1` (one member, also `c1`, reading the primary topic)
as well as a producer `p1`. The mapping removes the producer with `RP:p1@500` and
the group with `RG:c1@750` before anything else, so a fake consumer does not sit
beside the real groups, and it does so even when the cluster has no consumer
group at all.

*A real group called `c1`.* The template group is removed all the same, with
no exception and no warning. The real group is added fresh later in the log by its
normal `AG:c1:<its topics>` and its `AC` members, so it shows its real topics and
members and nothing of the template's. Replayed through the simulator's own
engine, `RP:p1@500,RG:c1@750,AG:c1:orders@1000,AC:c1:worker-1@1250` leaves one
group `c1` with exactly the member `worker-1`: `remove_group` takes the whole
group and its member `c1` away, the template's first consumer poll (t = 800 ms)
fires between the removal (750 ms) and the first possible `add_group` (1000 ms)
and dies on the missing group, and the re-added group schedules its own single
poll chain.

`fetchGroups` (`snapshot/kafka.go`) lists every group (`ListGroups`, filtered
to `ProtocolType == "consumer"` — Kafka Connect workers and KIP-932 share
groups are skipped), describes up to `MaxGroupsFetched` of them alphabetically
(`DescribeGroups`, for state and members) and fetches their committed offsets
restricted to the selected topics (`OffsetFetch`). All three requests are
read-only; none joins a group or commits an offset.

The mapping then, per real group (at most `MaxGroups` = 4, the ones with the
most lag when more qualify; mirroring `MAX_FREE_PLAY_GROUPS`, a self-imposed
canvas-legibility cap, not an engine limit):

1. Intersects the group's committed-offset topics with the rendered topic
   set. **No overlap → the group is skipped**; one warning lists every
   skipped group (the "committed offsets on a topic not selected"
   degrade-gracefully case).
2. Emits `AG:<simGroup>:<topic1>~<topic2>…` (`add_group`) for the surviving
   topics, without the optional reset code or settings field.
3. Sums `max(0, latestOffset - committedOffset)` over every partition the
   group has committed on. If that aggregate lag is `> 0`, emits
   `C:group.<simGroup>.consumer.autoOffsetReset:earliest` right after
   `add_group` — a real fresh group defaults to `latest` (starts at the high
   watermark), so this is the one lever the action-log grammar has to make a
   group visibly behind instead of caught up.
4. Joins up to `MaxGroupMembers` (= 4, mirroring `MAX_FREE_PLAY_GROUP_MEMBERS`)
   real members (the first 4 in the order `DescribeGroups` returned them, not
   sorted) via `AC:<simGroup>:<simMember>` (`add_consumer`), named from
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

## Producers: `--producers`, `AP`

Off by default. Without `--producers` the tool sends no `DescribeProducers`
request and the link has no producers, exactly as before. The flag makes
`fetchProducers` (`snapshot/kafka.go`) send one `DescribeProducers` request
(KIP-664, Kafka 2.8 or newer) for every partition of the selected topics, split
by partition leader. It is a read, but it needs **READ on each topic**, which is
why it is opt-in.

**This is not a list of every writer.** `DescribeProducers` reports a broker's
producer state, and a broker only has state for **idempotent and transactional**
producers. A producer with `enable.idempotence=false` and no transaction has no
producer id and never appears, however busy it is. What the link shows is the
idempotent and transactional producers that wrote lately, and nothing about the
rest. Three more limits shape that:

- The activity time is the **record timestamp** of the producer's last batch on
  the partition, not the arrival time: the producing client's clock on a
  `CreateTime` topic, the broker's clock on a `LogAppendTime` topic. A producer
  that stamps event time or replays historical records can look old (or, on a
  skewed clock, be in the future) however recently it wrote.
- Brokers keep producer state only for `producer.id.expiration.ms` (24 hours by
  default), so a `--producers-window` longer than that shows nothing extra.
- A producer id is one producer instance: a restarted application is a new id.

The reply lists, per partition, those producer ids with the timestamp of each
one's last write and the first offset of its open transaction, if any. The
mapping then:

1. **Counts a producer as active** on a partition when that timestamp is within
   `--producers-window` (default 15m) before the snapshot time, taken on the
   machine running the tool, so a large clock difference to the brokers or the
   producers shifts the window. A write with no timestamp is not counted. Only the
   topics and partitions the link renders count (after the topic and partition
   caps), because a producer needs a topic to be pinned to.
2. **Ranks** the active producers by how many partitions they wrote to, then by
   the most recent write, then by producer id, and keeps the top 4 (free play
   renders at most `MAX_FREE_PLAY_PRODUCERS` = 4, and the engine ignores an
   `add_producer` past 4 live ones). Over 4, one warning names the ones kept and
   dropped, like the group cap.
3. **Names** each `pid-<producerId>` (a Kafka producer id is a non-negative 64-bit
   integer, so the name is at most 23 characters, inside the 40 the other
   generated ids stay under, and clear of the template `p1` and the transient
   `kpN`).
4. **Pins** it to the rendered topic it wrote to on the most active partitions; a
   tie goes to the earlier topic in the usual order (`orders`, then
   alphabetical).
5. **Emits**, after everything else (the keyed replay and its transient
   producers are gone, so the replay is untouched, and the consumer groups are in
   place), one `AP:pid-<id>:<topic>` per producer and nothing more: the producer
   is created with the simulator's default config (`acks=all`, idempotence on), so
   there is no config change after it. The added producers are not removed again,
   so the playground's auto-loop, which is on by default in free play, produces
   from them: the link keeps showing writes from the producers it found. Their
   records come on top of the synthetic ones, so offsets keep growing.

**Idempotent, not transactional.** Every producer `DescribeProducers` lists is
idempotent or transactional, and the simulator's default for a new producer is
already idempotent (`acks=all`, idempotence on), so each is added with that
default config. A producer with an open transaction (`CurrentTxnStartOffset`
of 0 or more on some partition) cannot be shown as transactional: the URL grammar
has no `transactional.id` (it is not on the `config_change` whitelist, and the
size slug's producer list has no such field), and `begin_transaction` does
nothing for a producer without one, so a `BT` action would be ignored by the
engine without a trace. It is added as an idempotent producer and a warning
names it and the partitions. A transactional producer between transactions looks
like an idempotent one, so it is not told apart.

**When the read does not work** the link is still built, without the producers
that could not be read, and a warning says why: `UNSUPPORTED_VERSION` or a broker
too old to know the API (Kafka before 2.8), `CLUSTER_AUTHORIZATION_FAILED`,
`TOPIC_AUTHORIZATION_FAILED` (naming the topics; the producers of the topics that
were readable are still used), another error code on some partitions, or a read
that did not finish in 5 seconds. With nothing wrong but no idempotent or
transactional producer inside the window, a warning says so; that is not the same
as "nobody wrote".

## Schema Registry failures: `KSR`/`RSR`/`SFO`/`RPR`/`SRE` (v2.2 preview)

`connect` never emits these: it reads no Schema Registry. The MCP
`build_playground_url` tool writes them from its action list, and
`validate_actions` decodes them, with the playground's own codes
(playground-url-api.md §2):

| Action kind | Code | Notes |
|---|---|---|
| `kill_sr` | `KSR:<instanceId>` | stops one SR instance; the SR leader is re-elected on the same step |
| `restart_sr` | `RSR:<instanceId>` | |
| `sr_failover` | `SFO:<deploymentId>` | a follower-only deployment on a mirrored `_schemas` takes over writes; a no-op for any other deployment |
| `restart_producer` | `RPR:<producerId>[:n]` | restarts the producer with an empty schema cache; `:n` (`newSchema`) deploys the next schema version of its subject |
| `set_sr_eligibility` | `SRE:<instanceId>:<1\|0>` | `leader.eligibility`: `1` may lead, `0` follower-only |

The instances exist only in a session whose shape has
`schemaRegistry.instancesPerDc` >= 1. They are named `sr-<dc>-<n>` in one
deployment named `sr`: `sr-dc-1-1` on single-dc and diskless-3az (one DC,
named `dc-1`), `sr-dc-a-1` on stretched-3, `sr-primary-1` / `sr-dr-1` on
active-passive, `sr-west-1` / `sr-east-1` on active-active. The tool lists
them in its notes. Like broker ids, instance ids are not checked against the
shape: an id the session does not have is a no-op in the playground.

The playground reads an `RPR` flag only when it is `n` and ignores anything
else after the producer id, so the decoder does the same: `RPR:p1:x` previews
as a plain restart, not a drop. These codes are a v2.2 preview of the
simulator; a deployment of the playground older than v2.2 drops them.
`register_schema` (`RSV`) and `set_schema_compatibility` (`SSC`) are not
encodable here yet.

## Diskless implementation presets (v2.2)

`connect` never emits these: it maps a real cluster onto single-dc. The MCP
`build_playground_url` tool takes four more `cluster` values for the diskless
implementations the simulator models, each one cluster in a single zone with the
shared multi-DC caps (at most 9 brokers and 6 partitions per topic):

| `cluster` | Default `size` | What the shape accepts |
|---|---|---|
| `inkless-3az` | `6x3x3` | diskless-3az on Aiven Inkless: `orders` stays classic (no `disklessEnable` on it), the preset's `clicks` is diskless at RF 1, PostgreSQL in `az-a` is the batch coordinator. Both `disklessTiming` fields apply. |
| `kafscale-3b` | `3x3x1` | KafScale 2.0: every topic is diskless at RF 1 (S3 behind etcd leases): an RF above 1 or a compacted topic is refused, an extra topic gets the diskless `~1` without asking. `disklessTiming.uploadMs` only; `commitIntervalMs` is refused because KafScale has no commit interval. |
| `automq-3az` | `3x6x1` | every topic is S3-backed at RF 1 behind a per-broker WAL, with the same RF, compaction and `~1` rules as `kafscale-3b`. Both `disklessTiming` fields apply; `commitIntervalMs` is the WAL batch interval. |

The playground drops the primary diskless flag (`d`, tail index 17) on all
four, so the tool never writes it there. A topic named like a preset's own
topic (`clicks` on `diskless-3az` and `inkless-3az`) is refused, since the
playground would rename it. The slugs are pinned by the `SIZE-IMPL` lines in
`mapping/testdata/ts-fixtures.txt`, dumped from the simulator's own codec.

The AutoMQ zone router (size segment 17's `r`) has no shape input: the shape
describes no producers or consumer groups, so it cannot place clients in zones
for the router to matter.

Two action kinds stop and start the control plane of these presets
(playground-url-api.md §2). They take no fields and are a no-op on any other
cluster:

| Action kind | Code | Notes |
|---|---|---|
| `stop_coordinator_db` / `start_coordinator_db` | `DB:0` / `DB:1` | the Inkless PostgreSQL batch coordinator (`inkless-3az`), and from v2.3 the PostgreSQL storage engine of `nisshi-3b` (see *Nisshi* below) |
| `stop_etcd` / `start_etcd` | `ET:0` / `ET:1` | KafScale's etcd, which holds the metadata and partition leases (`kafscale-3b`) |
| `stop_control_plane` / `start_control_plane` | `CP:0` / `CP:1` | v2.3, `next`: WarpStream's hosted control plane (`warpstream-3az`; see below) |

Two more pause and resume one KafScale broker (and, from v2.3, an Ursa broker:
see *Ursa for Kafka* below). They take `brokerId` (the `<broker>` in the code)
and are a no-op on any cluster other than `kafscale-3b` and `ursa-3az`, on a
dead or paused broker (pause) and on a running one (resume):

| Action kind | Code | Notes |
|---|---|---|
| `pause_broker` | `BP:<broker>` | freezes the broker (SIGSTOP, a long GC pause): its requests time out and its partition leases expire after the 10 s TTL |
| `resume_broker` | `BU:<broker>` | wakes it (SIGCONT). If its etcd session expired meanwhile, it drops its partition logs and restores a partition before it writes it again (scenario 08.2.7) |

An empty broker id is an error here; the playground drops the entry.

Three more action kinds act on the object storage bucket behind every diskless
implementation: the KIP draft (`diskless-3az`), Inkless, KafScale and AutoMQ.
They take no fields and are a no-op on a cluster without diskless topics:

| Action kind | Code | Notes |
|---|---|---|
| `stop_object_storage` | `OS:0` | object storage becomes unavailable |
| `slow_object_storage` | `OS:s` | every object storage operation takes 2 s |
| `start_object_storage` | `OS:1` | object storage is available again (KafScale's S3 health monitor still refuses until 60 s after its last bad sample) |

Any other `OS` payload is an error here; the playground drops it.

A deployment of the playground older than v2.2 falls back to single-dc for
these `cluster` values and drops the codes.

## WarpStream and Redpanda Cloud Topics: `warpstream-3az`, `redpanda-3az` (v2.3, `next`)

The simulator's v2.3 adds WarpStream and Redpanda Cloud Topics, at release stage
`next`: the public playground does not offer the `freeplay.warpstream` and
`freeplay.redpanda` features yet and opens these `cluster` values as single-dc.
`connect` never emits them. The MCP `build_playground_url` tool takes them:

| `cluster` | Default `size` | What it opens |
|---|---|---|
| `warpstream-3az` | `3x3x1` | 3 WarpStream Agents, one per zone `az-a`, `az-b`, `az-c`, and no KRaft controllers: the control plane is hosted outside the account and assigns the offsets at commit. Every topic is in object storage at replication factor 1 (any Agent serves any partition); `orders` has 3 partitions. Producers `p-a` (orders, zone `az-a`, idempotent) and `p-c` (orders, zone `az-c`, not idempotent), both `acks=all`; group `g-orders` with member `c1` (zone `az-b`, `read_committed`) reads orders. Diskless timing 500 ms `batchTimeout`, 600 ms upload, 200 ms control plane commit. |
| `redpanda-3az` | `3x6x3` | 3 Redpanda v26.2.2 brokers cycling racks `az-a`, `az-b`, `az-c` (rack-aware) and no KRaft controllers (Redpanda's controller Raft group is not drawn). Every topic is a cloud topic by the cluster (`redpanda.storage.mode=cloud`) at its own replication factor: `orders` has 6 partitions at RF 3, leaders two per broker, so each L0 object mixes partitions, and a placeholder in each partition's Raft log gives a record its offset. Producers `p-all` (`acks=all`, idempotent) and `p-one` (`acks=1`, not idempotent), both in `az-a`; group `g-orders` with member `c1` (`az-b`) reads orders. Diskless timing 500 ms L0 upload interval, 600 ms upload, 40 ms L1 registration; the reconciler runs every 2 s. |

On `warpstream-3az` the shape refuses what the playground would rewrite:

- fewer than 3 or more than 6 Agents: one or two in each zone. Past one Agent
  per zone the playground stores the per-Agent zone list in tail index 4, so the
  tool writes the cycle there (`4x3x1xxxxxaz-a!az-b!az-c!az-a` on 4 Agents); the
  shape has no input for another assignment;
- a replication factor above 1 ("WarpStream partitions have no replicas; object
  storage replicates the data and any Agent serves any partition.");
- a compacted cleanup policy (WarpStream supports compacted topics, but the
  simulator does not model file compaction yet and creates the topic with
  `cleanup.policy=delete`);
- more than 6 partitions per topic.

As on KafScale and AutoMQ, an extra topic gets the diskless `~1` without asking
and the primary diskless flag is never written. Both `disklessTiming` fields
apply: `commitIntervalMs` is the Agent's `batchTimeout`.

On `redpanda-3az` the shape refuses:

- `disklessEnable: true` on any topic: every topic is a cloud topic by the
  cluster, and the playground drops the flag. A `disklessTiming` applies with no
  diskless topic; `commitIntervalMs` is the L0 upload interval
  (`cloud_topics_produce_upload_interval`);
- a compacted cleanup policy (Redpanda compacts cloud topics in L1, but the
  simulator does not model it yet and creates the topic with
  `cleanup.policy=delete`);
- more than 9 brokers, 6 partitions per topic, or a replication factor above
  the broker count.

With fewer than 3 brokers and no `orders` in the shape, `orders` follows the
broker count down, and a note says so.

The shape describes no clients, so it cannot set WarpStream Ripcord (the
Agent's `-enableRipcord`, a `c` in the size's diskless segment), a WarpStream
lightning topic (`warpstream.topic.type=lightning`, an extra topic's 11th field
`l`) or Redpanda's `enable_rack_awareness` (an `a` in the diskless segment); the
playground leaves all three off by default. `rackAware: true` is kept in the
slug on both, though `redpanda-3az` is rack-aware already.

Two action kinds stop and start the WarpStream control plane. They take no
fields and are a no-op on any other cluster:

| Action kind | Code | Notes |
|---|---|---|
| `stop_control_plane` | `CP:0` | the control plane is unreachable: commits in flight fail, produce and fetch get `REQUEST_TIMED_OUT`, group calls `COORDINATOR_NOT_AVAILABLE`, and an Agent cannot start |
| `start_control_plane` | `CP:1` | it is reachable again; with Ripcord on, journaled objects are replayed |

Any other `CP` payload is an error here; the playground drops it. The
playground's engine refuses some actions on these clusters (it logs
`admin_rejected` and changes nothing), and `build_playground_url` refuses them
up front with the engine's copy:

| Cluster | Action | Refused when | Engine copy |
|---|---|---|---|
| `warpstream-3az` | `stop_object_storage`, `slow_object_storage` | always (`start_object_storage` does nothing there) | WarpStream's docs do not describe what Agents do when object storage is unavailable or slow, so the simulator does not model it. |
| `warpstream-3az` | `restart_broker`, `add_broker` | a `stop_control_plane` comes before it in replay order (by `at`, ties in list order) with no `start_control_plane` in between | A WarpStream Agent cannot start while the control plane is unreachable. |
| `warpstream-3az` | `reassign_partition` to more than one Agent, `change_replication_factor` | always | WarpStream partitions have no replicas; object storage replicates the data and any Agent serves any partition. |
| `warpstream-3az` | `add_share_group` | always: every topic is diskless | WarpStream does not support share groups (its protocol support page lists no share-group APIs). |
| `redpanda-3az` | `remove_broker` | a starting broker, while a topic's replication factor equals the broker count (every broker then holds a replica of each of its partitions; the preset's `orders` at RF 3 on 3 brokers), unless a `reassign_partition` or `change_replication_factor` comes first. Otherwise replica placement decides, which this tool does not compute, so the response carries a note instead. | a broker that holds replicas of Redpanda partitions leaves them by a Raft membership change, which the simulator does not model; move its replicas off first, or kill it |

Object storage outages are modelled on Redpanda: `stop_object_storage`,
`slow_object_storage` and `start_object_storage` apply there. The slugs are
pinned by the `SIZE-IMPL warpstream-3az` and `SIZE-IMPL redpanda-3az` lines in
`mapping/testdata/ts-fixtures.txt`, dumped from the simulator's codec.

`open_scenario` takes both tracks, also at release stage `next`. WarpStream,
10.1.1-10.1.7: 10.1.1 the write path, 10.1.2 zones and a leader per client,
10.1.3 an Agent dies, 10.1.4 the control plane is unreachable, 10.1.5 Ripcord,
10.1.6 lightning topics, 10.1.7 transactions in the control plane. Redpanda
Cloud Topics, 11.1.1-11.1.5: 11.1.1 the write path, 11.1.2 the leader crashes,
an election, nothing to replay, 11.1.3 object storage goes down, 11.1.4 from L0
to L1 and who serves a read, 11.1.5 exactly-once on Cloud Topics.

## Ursa for Kafka: `ursa-3az` (v2.3, `next`)

The simulator's v2.3 adds StreamNative Ursa for Kafka, at release stage `next`:
the public playground does not offer the `freeplay.ursa` feature yet and opens
`cluster=ursa-3az` as single-dc. `connect` never emits it. The MCP
`build_playground_url` tool takes `cluster: "ursa-3az"`:

| `cluster` | Default `size` | What it opens |
|---|---|---|
| `ursa-3az` | `3x3x1` and the flag `d` (omitted from the URL) | 3 Ursa brokers cycling racks `az-a`, `az-b`, `az-c` (rack-aware) and 3 KRaft controllers. `orders` is diskless (`ursa.storage.enable=true`): 3 partitions at replication factor 1; a producer writes through a zonal owner in its own zone, the owner buffers many partitions into one WAL object, Oxia assigns the offsets at commit, and a compaction service rewrites WAL objects as Parquet every 2 s. The preset's `payments` is a disk topic: 3 partitions, RF 3, `min.insync.replicas` 2. Producers `p-a` (orders, `client.id` `zone_id=az-a`, idempotent), `p-c` (orders, `zone_id=az-c`, not idempotent) and `p-pay` (payments, `zone_id=az-b`, idempotent), all `acks=all`; group `g-orders` with member `c1` (`zone_id=az-b`) reads orders. Diskless timing 500 ms buffer close, 600 ms upload, 200 ms Oxia commit. |

Ursa chooses per topic, so `disklessEnable` is `ursa.storage.enable` there:
`orders` is diskless unless the shape sets it to `false`, and any other topic is
a disk topic unless the shape sets it to `true`. A client's zone is its
`zone_id` inside `client.id`, not `client.rack`; the shape describes no clients,
so it sets none. In the size slug, the primary diskless flag (`d`, tail index
17) is what makes `orders` diskless: a slug without it opens a disk `orders`, so
every shaped slug with a diskless `orders` carries it (`4x3x1xxxxxxxxxxxxxxxxxxd`
on 4 brokers). The shape refuses what the playground would rewrite:

- a diskless topic with a replication factor above 1 ("Ursa forces replication
  factor 1 on diskless topics; object storage holds the data");
- a diskless topic with a compacted cleanup policy (Ursa does not support
  compacted diskless topics yet; disk topics support them);
- more than 6 brokers (two per zone) or 6 partitions per topic, and an extra
  topic named `payments`;
- `disklessTiming` when no topic is diskless. Both fields apply:
  `commitIntervalMs` is the write buffer close, `uploadMs` the WAL upload.

With fewer than 3 brokers the playground creates `payments` at a replication
factor equal to the broker count, and a note says so.

The broker actions apply as on any cluster: `kill_broker`, `restart_broker`
and, with the KafScale codes `BP`/`BU`, `pause_broker` and `resume_broker`. A
paused Ursa broker keeps its diskless partitions for 5 s of missed heartbeats,
then is marked offline and its clients move to another owner. The playground's
engine refuses some actions on `ursa-3az` (it logs `admin_rejected` with
`InvalidConfigurationException` and changes nothing), and
`build_playground_url` refuses them up front with the engine's copy:

| Action | Refused when | Engine copy |
|---|---|---|
| `stop_object_storage`, `slow_object_storage` | always (`start_object_storage` does nothing there) | Ursa's sources do not describe what brokers do when object storage is unavailable or slow, so the simulator does not model it. |
| `pause_broker` | the broker holds any replica of a disk-topic partition. On a starting cluster of at most 3 brokers every starting broker holds all of `payments`, so the pause is refused unless a `reassign_partition` or `change_replication_factor` comes first; add a broker with `add_broker` and pause that one. On more brokers the outcome depends on replica placement, which this tool does not compute, so the response carries a note instead. | The simulator pauses Ursa brokers that hold only diskless partitions; `<broker>` holds disk-topic replicas (...) |
| `add_share_group` | its topic is diskless | Share groups on diskless topics are refused as the simulator's limit (Ursa supports them). |

The engine also refuses network partitions and an Oxia outage on Ursa; sim-link
has no action kind for either. The slugs are pinned by the `SIZE-IMPL ursa-3az`
lines in `mapping/testdata/ts-fixtures.txt`, dumped from the simulator's codec.

`open_scenario` takes the Ursa for Kafka track, 13.1.1-13.1.5, also at release
stage `next`: 13.1.1 the write path, 13.1.2 zones and the zonal owner, 13.1.3
from WAL to Parquet, 13.1.4 a broker dies and a broker goes silent, 13.1.5
diskless and disk topics side by side.

## Nisshi: `nisshi-3b` (v2.3, not released yet)

The simulator's v2.3 adds Nisshi (formerly Tansu), which is not released yet:
the public playground does not offer the `freeplay.nisshi` feature and opens
`cluster=nisshi-3b` as single-dc. `connect` never emits it, and refuses a live
Nisshi cluster (see *What is NOT represented*). The MCP `build_playground_url`
tool takes `cluster: "nisshi-3b"`:

| `cluster` | Default `size` | What it opens |
|---|---|---|
| `nisshi-3b` | `3x3x1` | 3 Nisshi brokers, no zones and no KRaft controllers. Every broker answers as node 111, the leader and only replica of every partition, so any broker serves any request; the storage engine holds every record, offset and group. `orders` has 3 partitions at replication factor 1. Producer `p-orders` (orders, `acks=all`, idempotent) and group `g-orders` with member `c1` read and write through different brokers. The engine is PostgreSQL (one transaction per write, holding the partition's watermark row lock; a maintenance sweep every 10 s applies retention and compaction) unless the shape picks S3 (a conditional write of the partition's `watermark.json`, then the batch as its own object; Nisshi's docs call the engine experimental). |

The shape's `nisshi` object picks the engine, written into tail index 17 the
way the simulator's codec writes it:

| Field | Values | Slug |
|---|---|---|
| `nisshi.engine` | `postgres` (default) or `s3` | `s` for S3 (`3x3x1xxxxxxxxxxxxxxxxxxs`); nothing for PostgreSQL |
| `nisshi.maintenanceMs` | 2000-600000, PostgreSQL only (the preset's 10000 when unset; Nisshi's own default is 600000) | `m<ms>` after the timing (`3x3x1xxxxxxxxxxxxxxxxxxm30000`) |
| `disklessTiming.uploadMs` | 100-5000 | `.<ms>` after the `s` (`3x3x1xxxxxxxxxxxxxxxxxxs.900`): the S3 engine's batch PUT. On PostgreSQL the slug keeps it, as the playground does, and a note says the engine does not use it |

Every topic is a Nisshi topic: an extra topic carries the diskless `~1` without
asking, and the primary diskless flag `d` is never written. Following the
playground rather than refusing, a replication factor above 1 is built as 1
(with `min.insync.replicas` 1), and a note quotes the simulator: "Every Nisshi
broker reports itself as the only replica (node 111) and serves every
partition; the storage engine holds the data." A compacted cleanup policy is
taken on either engine; on S3 a note says it is stored but not applied (only
Nisshi's PostgreSQL and SQLite engines apply retention and compaction). The
shape refuses:

- `nisshi.maintenanceMs` with the S3 engine (it runs no maintenance sweep) or
  outside 2000-600000, and an engine other than `postgres` or `s3`;
- `disklessTiming.commitIntervalMs` (the simulator writes each Nisshi produce
  request to the storage engine with no buffer interval);
- more than 5 brokers or 6 partitions per topic;
- `nisshi` on any other cluster.

The storage engine is stopped and started with the existing codes: on
PostgreSQL `stop_coordinator_db` / `start_coordinator_db` (`DB:0` / `DB:1`), on
S3 `stop_object_storage`, `slow_object_storage` and `start_object_storage`
(`OS:0`, `OS:s`, `OS:1`). While the engine is down, group requests and offset
commits answer COORDINATOR_NOT_AVAILABLE, since the engine holds the groups and
offsets. A `DB` code on S3 and `start_object_storage`
on PostgreSQL do nothing, and a note says so. The playground's engine refuses
some actions on `nisshi-3b` (it logs `admin_rejected`, or `config_rejected` for
a config change, and changes nothing), and `build_playground_url` refuses them
up front with the engine's copy:

| Action | Refused when | Engine copy |
|---|---|---|
| `pause_broker` | always | A Nisshi broker keeps no state, so a paused one only makes its clients time out; the simulator does not model pausing a Nisshi broker. |
| `stop_object_storage`, `slow_object_storage` | the engine is PostgreSQL | Nisshi's PostgreSQL engine keeps every record in PostgreSQL, not in object storage: stop PostgreSQL instead. |
| `reassign_partition` | always | Every Nisshi broker reports itself as the only replica (node 111) and serves every partition; the storage engine holds the data. |
| `change_replication_factor` | the target is above 1 (a target of 1 changes nothing) | the same |
| `add_share_group` | always | Nisshi has no share group APIs (KIP-932). |
| `config_change` `topics.<t>.partitions` | the value is above the topic's partition count in the shape | Nisshi does not implement CreatePartitions, so a topic keeps its partition count (UNSUPPORTED_VERSION). |
| `config_change` `topics.<t>.remoteStorageEnable` / `disklessEnable` | `true` / any value | Nisshi has one kind of topic; its storage engine holds every record. |

The engine also refuses network partitions and transactional producers on
Nisshi (a producer with a `transactional.id` stops with
TRANSACTIONAL_ID_AUTHORIZATION_FAILED); sim-link has no action kind for a
network partition and none that makes a producer transactional. The slugs are
pinned by the `SIZE-IMPL nisshi-3b` lines in `mapping/testdata/ts-fixtures.txt`,
dumped from the simulator's codec.

`open_scenario` takes the Nisshi track, 12.1.1-12.1.7, also at v2.3: 12.1.1 the
write path, 12.1.2 on PostgreSQL: one transaction per write, 12.1.3 two
brokers, one partition, 12.1.4 reading through another broker, 12.1.5 a broker
dies, 12.1.6 PostgreSQL unavailable, 12.1.7 what it refuses.

## Streamhouse: `IO`/`KCW`/`RCW`/`RCN` and the Connect sink (v3.0, `next`)

Simulator v3.0 adds the Streamhouse free-play cluster: a classic Kafka cluster
with Kafka Connect and Apache Iceberg's sink connector beside it. It is at
release stage `next`: the public playground does not ship the `streamhouse`
feature yet, so it opens `cluster=streamhouse` as single-dc and skips the four
codes below like an unknown kind. sim-link writes and reads them now, from the
grammar in playground-url-api.md (§2, §10, §11.3; website PR #977).

`connect` never emits any of this. The MCP `build_playground_url` tool takes
`cluster: "streamhouse"`:

| `cluster` | Default `size` | What it opens |
|---|---|---|
| `streamhouse` | `3x3x3` | 3 brokers, `orders` 3 partitions RF 3, a Schema Registry, and 2 Connect workers (`connect-1`, `connect-2`) running the Iceberg sink connector `orders-sink` (`tasks.max` 2, commit interval 2 s, timeout 6 s, schema evolution on) into the Iceberg table `orders` |

The shape takes the shared multi-DC caps (9 brokers, 6 partitions per topic)
and refuses a diskless topic or timing (the sink runs on classic topics only)
and an extra topic named `control-iceberg` (the sink's control topic, which the
playground would rename). `shape.connectSink` sets the sink, size tail index 18
(the 19th tail segment), `<workers>.<tasksMax>.<commitIntervalMs>.<commitTimeoutMs>.<0|1>`:

| Field | Range | Preset |
|---|---|---|
| `workers` | 1-4 | 2 |
| `tasksMax` | 1 to the partitions of `orders` | 2 |
| `commitIntervalMs` (`iceberg.control.commit.interval-ms`) | 1000-30000, steps of 500 | 2000 |
| `commitTimeoutMs` (`iceberg.control.commit.timeout-ms`) | 2000-60000, steps of 500 | 6000 |
| `evolveSchema` (`iceberg.tables.evolve-schema-enabled`) | `true` / `false` | `true` |

A value outside its range is an error here; the playground would clamp it. A
sink equal to the preset's adds nothing to the slug, but an unset `tasksMax` on
a 1-partition `orders` is lowered to 1, as the playground does, and then the
segment is written (`3x1x3xxxxxxxxxxxxxxx1xxxx2.1.2000.6000.1`).

**The registry.** The preset has a Schema Registry, but a slug without tail
index 14 decodes without one, so `size=4x3x3` opens Streamhouse with no
registry. The tool always keeps the preset's registry: every slug it writes for
`streamhouse` carries at least `1` at index 14 (`4x3x3xxxxxxxxxxxxxxx1`), and
`shape.schemaRegistry.instancesPerDc` adds SR instances as on any cluster. It
cannot turn the registry off. The §11.3 example
`3x3x3xxxxxxxxxxxxxxxxxxx3.3.5000.10000.0` has no registry for that reason; the
tool writes that sink as `3x3x3xxxxxxxxxxxxxxx1xxxx3.3.5000.10000.0`. The slugs
are pinned by the `SIZE-SINK` lines in `mapping/testdata/ts-fixtures.txt`,
dumped from the simulator's own codec.

Four action kinds act on the cluster's Iceberg table and Connect sink. Each
payload is escaped like a produce value (`~` becomes `~t`, `,` becomes `~c`):

| Action kind | Code | Fields | Notes |
|---|---|---|---|
| `iceberg_op` | `IO:<op request as JSON>` | `icebergOp` | one Iceberg table operation, e.g. `{"op":"set_properties","table":"orders","updates":{...},"removals":[]}` |
| `kill_connect_worker` | `KCW:<workerId>` | `workerId` | the worker dies; its tasks move to the surviving workers after the rebalance delay |
| `restart_connect_worker` | `RCW:<workerId>` | `workerId` | the worker starts again and rejoins the Connect group |
| `restart_connector` | `RCN:<name>` | `connectorName` | restarts the sink connector (the preset's is `orders-sink`), for example after it failed |

The `op` of an Iceberg op request is one of `create_table`, `append`, `delete`,
`update`, `merge`, `equality_delete`, `rewrite_data_files`,
`rewrite_position_delete_files`, `rewrite_manifests`, `create_tag`,
`create_branch`, `replace_ref`, `drop_ref`, `fast_forward`, `rollback`,
`set_current`, `cherrypick`, `expire_snapshots`, `remove_orphan_files`,
`set_properties`, `upgrade_format`, `update_schema`, `update_spec`,
`replace_sort_order`, `catalog_down` and `catalog_up`. The playground skips a
request that does not have exactly the keys and JSON types of its operation,
so the codec ports that check (`parseIcebergOpRequest`) and refuses such a
request on encode and on decode. It writes the JSON the way the playground
writes it back (`JSON.stringify`: no whitespace, the request's key order,
JavaScript's number form), so a link it decodes re-encodes byte for byte. The
MCP input `icebergOp` is a JSON object, whose key order does not survive the
call, so the tool writes its keys sorted. An empty worker id or connector name
is an error here; the playground drops the entry.

## Self-validation

`Map` verifies its own output round-trips the codec
(`encode → decode → encode`), and the CLI re-decodes the final URL value and
refuses URLs over 2048 chars - `--force` downgrades that to a warning. The
compressed `~1` form is accepted, not refused: the playground decodes both the
raw-DEFLATE layout this tool emits and its own lz-string layout (see
[Self-validation](https://monedula.dev/flock/docs/sim-link/concepts/self-validation/)). Losslessness is never overridable.

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
- **Groups/members beyond the caps.** At most `MaxGroups` (4) real groups
  (the ones with the most lag) and `MaxGroupMembers` (4) real members per group
  (the first 4 in `DescribeGroups` order, not sorted) are rendered - the same
  canvas-legibility caps the free-play UI itself uses.
- **Controller / KRaft state.** The simulator boots its own 3-voter quorum;
  the real controller id and quorum layout are ignored.
- **Rack topology.** The single-DC free-play shape has no rack axis; real
  broker racks are captured in the snapshot but not encoded. (The simulator's
  rack model is a size-param extension the mapping does not emit.)
- **Per-client `client.rack`** (v2.0+, KIP-1163 draft). The TypeScript codec
  encodes an optional rack on each initial producer / consumer group in the
  size slug (`freePlayTopologies.ts`, tail indices 1 and 2). Neither `Map` nor
  the MCP declarative shape (`mcpserver.ClusterShape`) writes those lists: the
  producers (`--producers`) and groups it adds are action-log entries
  (`add_producer`, `add_group`), and an `add_producer` has no rack field, so
  this stays unrepresented.
- **Producers that are not idempotent or transactional.** They have no producer
  id on the broker, so `--producers` cannot see them at all (see *Producers*).
- **Producer settings and transactions.** `--producers` shows some of who writes,
  not how: a producer's `acks`, linger, batching and the like are not read (the
  broker does not report them), so every added producer has the simulator's
  default config (`acks=all`, idempotence on), and a transactional producer is
  shown as an idempotent one (see *Producers*). Only up to 4 are shown, ranked by how many
  partitions they wrote to.
- **Remote-storage tiering config.** `remote.storage.enable` and
  `local.retention.ms` are never fetched, even though `min.insync.replicas` /
  `cleanup.policy` / `retention.ms` / `retention.bytes` now are.
- **Diskless topics (v2.0+, KIP-1163 draft).** `diskless.enable` is never
  fetched either - it has no real `DescribeConfigs` equivalent today. The
  codec can encode it (see "Cluster shape" above), but `Map` never sets it.
- **Schema Registry.** `connect` reads no real registry, subjects or SR
  instances; only the MCP declarative shape writes the registry segment (see
  "Cluster shape" above).
- **Real leadership.** See the placement approximation note.
- **Live traffic.** The snapshot is a point in time; the URL replays a static
  reconstruction, not a stream.
- **A Nisshi cluster.** Every Nisshi broker reports itself as node 111 in
  Metadata, the leader and only replica of every partition, so a snapshot sees
  one broker whatever the cluster runs and cannot tell which broker answered.
  When Metadata lists that single broker 111, the snapshot sends one more
  ApiVersions request: Nisshi offers no CreatePartitions (API key 37), every
  Kafka broker since 1.0 does. For Nisshi, `connect` and `cluster_to_url` stop
  with an error (`snapshot.ErrNisshi`) that points at `build_playground_url`
  with `cluster: "nisshi-3b"` and `shape.brokers` set to the broker count. A
  Kafka broker with `node.id` 111 is mapped as usual; if the ApiVersions
  request fails, it is mapped as a Kafka broker with a warning.
