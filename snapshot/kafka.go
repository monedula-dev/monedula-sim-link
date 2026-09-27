package snapshot

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// This file is the ONLY place monedula-sim-link talks to a real cluster, and it is
// STRICTLY READ-ONLY by construction: the only Kafka requests it builds are
// Metadata (with AllowAutoTopicCreation left false, so listing a topic can
// never create it), ListOffsets, ListGroups, DescribeGroups, OffsetFetch and
// DescribeConfigs; franz-go itself adds the ApiVersions handshake. All six are
// read requests — none of them is a JoinGroup/SyncGroup/Heartbeat (no group is
// ever joined), an OffsetCommit (no offset is ever committed), or an
// AlterConfigs/IncrementalAlterConfigs (no config is ever changed). No records
// are produced and no topic is ever created, altered or deleted.

// TLSConfig enables TLS on the bootstrap connections. All fields optional:
// zero value = system trust roots, no client certificate.
type TLSConfig struct {
	CACert     string // PEM file appended to the system pool
	ClientCert string // PEM client certificate (mTLS), requires ClientKey
	ClientKey  string
}

// SASLConfig enables SASL authentication.
type SASLConfig struct {
	Mechanism string // "plain", "scram-sha-256" or "scram-sha-512"
	Username  string
	Password  string
}

// ConnectConfig describes how to reach the cluster.
type ConnectConfig struct {
	Brokers []string   // bootstrap host:port list
	TLS     *TLSConfig // nil = plaintext
	SASL    *SASLConfig
}

// Client is a read-only connection to a Kafka cluster.
type Client struct {
	kc *kgo.Client
}

// Connect dials the cluster. Close the client when done.
func Connect(cfg ConnectConfig) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no bootstrap brokers given")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.TLS != nil {
		tc, err := buildTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	if cfg.SASL != nil {
		mech, err := buildSASL(cfg.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, mech)
	}
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Client{kc: kc}, nil
}

// Close releases the connection.
func (c *Client) Close() { c.kc.Close() }

func buildTLS(cfg *TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read --ca-cert: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--ca-cert %s holds no usable PEM certificates", cfg.CACert)
		}
		tc.RootCAs = pool
	}
	if cfg.ClientCert != "" || cfg.ClientKey != "" {
		if cfg.ClientCert == "" || cfg.ClientKey == "" {
			return nil, fmt.Errorf("--client-cert and --client-key must be given together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

func buildSASL(cfg *SASLConfig) (kgo.Opt, error) {
	if cfg.Username == "" {
		return nil, fmt.Errorf("--sasl-mechanism requires --username")
	}
	switch strings.ToLower(cfg.Mechanism) {
	case "plain":
		return kgo.SASL(plain.Auth{User: cfg.Username, Pass: cfg.Password}.AsMechanism()), nil
	case "scram-sha-256":
		return kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism()), nil
	case "scram-sha-512":
		return kgo.SASL(scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("unsupported --sasl-mechanism %q (plain, scram-sha-256, scram-sha-512)", cfg.Mechanism)
	}
}

// Fetch takes the point-in-time snapshot: one Metadata request for brokers +
// topics + partition placement, then the selection filter, then ListOffsets
// (earliest and latest) for every selected partition. Returns the snapshot,
// non-fatal warnings (partitions whose offsets could not be listed, topics
// with metadata errors), and an error when the selection matches nothing.
func (c *Client) Fetch(ctx context.Context, sel Selection) (ClusterSnapshot, []string, error) {
	var warnings []string
	snap := ClusterSnapshot{}

	metaReq := kmsg.NewPtrMetadataRequest()
	// Topics nil = all topics; AllowAutoTopicCreation stays false (zero value),
	// so metadata for a missing exact topic can never create it.
	meta, err := metaReq.RequestWith(ctx, c.kc)
	if err != nil {
		return snap, nil, fmt.Errorf("metadata request: %w", err)
	}

	for _, b := range meta.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		snap.Brokers = append(snap.Brokers, Broker{
			ID:     strconv.FormatInt(int64(b.NodeID), 10),
			Rack:   rack,
			Online: true,
		})
	}

	infos := make([]TopicInfo, 0, len(meta.Topics))
	byName := make(map[string]kmsg.MetadataResponseTopic, len(meta.Topics))
	for _, t := range meta.Topics {
		if t.Topic == nil || *t.Topic == "" {
			continue
		}
		name := *t.Topic
		if t.ErrorCode != 0 {
			warnings = append(warnings, fmt.Sprintf("topic %s: metadata error code %d; skipped", name, t.ErrorCode))
			continue
		}
		infos = append(infos, TopicInfo{Name: name, Internal: t.IsInternal || strings.HasPrefix(name, "_")})
		byName[name] = t
	}

	selected, err := sel.Apply(infos)
	if err != nil {
		return snap, warnings, err
	}

	for _, name := range selected {
		mt := byName[name]
		topic := Topic{Name: name}
		for _, p := range mt.Partitions {
			if p.ErrorCode != 0 {
				warnings = append(warnings, fmt.Sprintf("%s/%d: metadata error code %d (keeping placement, offsets may be zero)", name, p.Partition, p.ErrorCode))
			}
			part := Partition{ID: int(p.Partition)}
			if p.Leader >= 0 {
				part.Leader = strconv.FormatInt(int64(p.Leader), 10)
			}
			for _, r := range p.Replicas {
				part.Replicas = append(part.Replicas, strconv.FormatInt(int64(r), 10))
			}
			for _, r := range p.ISR {
				part.ISR = append(part.ISR, strconv.FormatInt(int64(r), 10))
			}
			topic.Partitions = append(topic.Partitions, part)
		}
		sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].ID < topic.Partitions[j].ID })
		snap.Topics = append(snap.Topics, topic)
	}

	earliest, w1, err := c.listOffsets(ctx, snap.Topics, -2)
	if err != nil {
		return snap, warnings, fmt.Errorf("list earliest offsets: %w", err)
	}
	latest, w2, err := c.listOffsets(ctx, snap.Topics, -1)
	if err != nil {
		return snap, warnings, fmt.Errorf("list latest offsets: %w", err)
	}
	warnings = append(warnings, w1...)
	warnings = append(warnings, w2...)
	for ti := range snap.Topics {
		t := &snap.Topics[ti]
		for pi := range t.Partitions {
			p := &t.Partitions[pi]
			p.EarliestOffset = earliest[t.Name][p.ID]
			p.LatestOffset = latest[t.Name][p.ID]
		}
	}

	if cfgWarnings := c.fetchTopicConfigs(ctx, snap.Topics); len(cfgWarnings) > 0 {
		warnings = append(warnings, cfgWarnings...)
	}
	groups, groupWarnings, err := c.fetchGroups(ctx, snap.Topics)
	if err != nil {
		return snap, warnings, fmt.Errorf("list consumer groups: %w", err)
	}
	snap.Groups = groups
	warnings = append(warnings, groupWarnings...)

	// Mark referenced-but-unregistered brokers offline for the mapping layer:
	// synthesize entries so the snapshot is self-contained.
	known := map[string]bool{}
	for _, b := range snap.Brokers {
		known[b.ID] = true
	}
	for _, t := range snap.Topics {
		for _, p := range t.Partitions {
			for _, id := range append(append([]string{}, p.Replicas...), p.ISR...) {
				if !known[id] {
					known[id] = true
					snap.Brokers = append(snap.Brokers, Broker{ID: id, Online: false})
				}
			}
		}
	}
	sort.Slice(snap.Brokers, func(i, j int) bool {
		a, errA := strconv.Atoi(snap.Brokers[i].ID)
		b, errB := strconv.Atoi(snap.Brokers[j].ID)
		if errA == nil && errB == nil {
			return a < b
		}
		return snap.Brokers[i].ID < snap.Brokers[j].ID
	})
	return snap, warnings, nil
}

// listOffsets issues one ListOffsets request (timestamp -2 = earliest, -1 =
// latest) for every partition of the given topics. franz-go splits it into one
// shard per partition leader. A shard that fails - typically a partition with
// no leader because its only replicas are down - becomes a warning for each of
// its partitions (offset assumed 0), so one offline partition cannot fail the
// whole snapshot. When every shard fails the first error is returned.
func (c *Client) listOffsets(ctx context.Context, topics []Topic, timestamp int64) (map[string]map[int]int64, []string, error) {
	out := make(map[string]map[int]int64, len(topics))
	if len(topics) == 0 {
		return out, nil, nil
	}
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	for _, t := range topics {
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = t.Name
		for _, p := range t.Partitions {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition = int32(p.ID)
			rp.CurrentLeaderEpoch = -1
			rp.Timestamp = timestamp
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		out[t.Name] = make(map[int]int64, len(t.Partitions))
	}
	var warnings []string
	kind := "latest"
	if timestamp == -2 {
		kind = "earliest"
	}
	var firstErr error
	succeeded := false
	for _, shard := range c.kc.RequestSharded(ctx, req) {
		if shard.Err != nil {
			if firstErr == nil {
				firstErr = shard.Err
			}
			shardReq, ok := shard.Req.(*kmsg.ListOffsetsRequest)
			if ctx.Err() != nil || !ok {
				return nil, nil, shard.Err
			}
			for _, t := range shardReq.Topics {
				for _, p := range t.Partitions {
					warnings = append(warnings, fmt.Sprintf("%s/%d: %s offset unavailable (%v); assuming 0", t.Topic, p.Partition, kind, shard.Err))
				}
			}
			continue
		}
		resp, ok := shard.Resp.(*kmsg.ListOffsetsResponse)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected ListOffsets response type %T", shard.Resp)
		}
		succeeded = true
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if p.ErrorCode != 0 {
					warnings = append(warnings, fmt.Sprintf("%s/%d: %s offset error code %d; assuming 0", t.Topic, p.Partition, kind, p.ErrorCode))
					continue
				}
				out[t.Topic][int(p.Partition)] = p.Offset
			}
		}
	}
	if !succeeded && firstErr != nil {
		return nil, nil, firstErr
	}
	return out, warnings, nil
}

// topicConfigNames are the four DescribeConfigs entries the mapping
// understands (docs/mapping.md); requesting exactly these keeps the read
// scoped to what is actually used instead of pulling every dynamic/static
// config Kafka knows about a topic.
var topicConfigNames = []string{"cleanup.policy", "min.insync.replicas", "retention.ms", "retention.bytes"}

// fetchTopicConfigs issues one DescribeConfigs request covering every given
// topic (read-only: DescribeConfigs never mutates) and fills in each Topic's
// Config in place. A per-topic error (e.g. authorization) is non-fatal: it is
// reported as a warning and that topic's Config is left zero-valued, which the
// mapping treats as "unknown, use the simulator's default".
func (c *Client) fetchTopicConfigs(ctx context.Context, topics []Topic) []string {
	if len(topics) == 0 {
		return nil
	}
	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, t := range topics {
		res := kmsg.NewDescribeConfigsRequestResource()
		res.ResourceType = kmsg.ConfigResourceTypeTopic
		res.ResourceName = t.Name
		res.ConfigNames = append([]string(nil), topicConfigNames...)
		req.Resources = append(req.Resources, res)
	}
	resp, err := req.RequestWith(ctx, c.kc)
	if err != nil {
		return []string{fmt.Sprintf("describe configs: %v; topic configs will use the simulator's defaults", err)}
	}
	byName := make(map[string]*Topic, len(topics))
	for i := range topics {
		byName[topics[i].Name] = &topics[i]
	}
	var warnings []string
	for _, res := range resp.Resources {
		t, ok := byName[res.ResourceName]
		if !ok {
			continue
		}
		if res.ErrorCode != 0 {
			warnings = append(warnings, fmt.Sprintf("topic %s: describe configs error code %d; using the simulator's defaults", res.ResourceName, res.ErrorCode))
			continue
		}
		for _, cfg := range res.Configs {
			if cfg.Value == nil {
				continue
			}
			switch cfg.Name {
			case "cleanup.policy":
				t.Config.CleanupPolicy = *cfg.Value
			case "min.insync.replicas":
				if n, err := strconv.Atoi(*cfg.Value); err == nil {
					t.Config.MinInSyncReplicas = &n
				}
			case "retention.ms":
				// -1 is Kafka's own sentinel for "disabled" (retain forever);
				// the simulator's equivalent is simply no override, so treat
				// any non-positive value as unset rather than passing a
				// negative number through to the URL.
				if n, err := strconv.ParseInt(*cfg.Value, 10, 64); err == nil && n > 0 {
					t.Config.RetentionMs = &n
				}
			case "retention.bytes":
				// Same -1-means-disabled convention as retention.ms.
				if n, err := strconv.ParseInt(*cfg.Value, 10, 64); err == nil && n > 0 {
					t.Config.RetentionBytes = &n
				}
			}
		}
	}
	return warnings
}

// MaxGroupsFetched bounds how many consumer groups fetchGroups describes and
// fetches offsets for. A cluster's group count is unbounded and unrelated to
// its topic/broker count, so without a cap a busy cluster could turn one
// snapshot into thousands of DescribeGroups/OffsetFetch round trips; the
// mapping layer separately clamps to a much smaller on-canvas count
// (mapping.MaxGroups), so this only guards the read itself.
const MaxGroupsFetched = 200

// fetchGroups lists every consumer group (ListGroups, read-only), describes
// the first MaxGroupsFetched of them alphabetically (DescribeGroups) for
// their state and members, then fetches their committed offsets restricted to
// the given topics (OffsetFetch) — all read-only requests; none joins a group
// or commits an offset. Groups whose protocol type is not "consumer" (Kafka
// Connect workers, KIP-932 share groups, …) are skipped, since they are not
// consumer groups the simulator can represent.
func (c *Client) fetchGroups(ctx context.Context, topics []Topic) ([]Group, []string, error) {
	if len(topics) == 0 {
		return nil, nil, nil
	}
	lgReq := kmsg.NewPtrListGroupsRequest()
	lgResp, err := lgReq.RequestWith(ctx, c.kc)
	if err != nil {
		return nil, nil, fmt.Errorf("list groups: %w", err)
	}
	var ids []string
	for _, g := range lgResp.Groups {
		if g.ProtocolType != "consumer" {
			continue
		}
		ids = append(ids, g.Group)
	}
	if len(ids) == 0 {
		return nil, nil, nil
	}
	sort.Strings(ids)
	var warnings []string
	if len(ids) > MaxGroupsFetched {
		warnings = append(warnings, fmt.Sprintf("cluster has %d consumer groups; reading at most %d (alphabetically first) to bound the snapshot", len(ids), MaxGroupsFetched))
		ids = ids[:MaxGroupsFetched]
	}

	dgReq := kmsg.NewPtrDescribeGroupsRequest()
	dgReq.Groups = ids
	dgResp, err := dgReq.RequestWith(ctx, c.kc)
	if err != nil {
		return nil, warnings, fmt.Errorf("describe groups: %w", err)
	}

	ofReq := kmsg.NewPtrOffsetFetchRequest()
	for _, id := range ids {
		g := kmsg.NewOffsetFetchRequestGroup()
		g.Group = id
		for _, t := range topics {
			ofg := kmsg.NewOffsetFetchRequestGroupTopic()
			ofg.Topic = t.Name
			for _, p := range t.Partitions {
				ofg.Partitions = append(ofg.Partitions, int32(p.ID))
			}
			g.Topics = append(g.Topics, ofg)
		}
		ofReq.Groups = append(ofReq.Groups, g)
	}
	ofResp, err := ofReq.RequestWith(ctx, c.kc)
	if err != nil {
		return nil, warnings, fmt.Errorf("offset fetch: %w", err)
	}
	offsetsByGroup := make(map[string]map[string]map[int]int64, len(ids))
	for _, og := range ofResp.Groups {
		if og.ErrorCode != 0 {
			warnings = append(warnings, fmt.Sprintf("group %s: offset fetch error code %d; committed offsets unavailable", og.Group, og.ErrorCode))
			continue
		}
		offs := make(map[string]map[int]int64, len(og.Topics))
		for _, ot := range og.Topics {
			parts := make(map[int]int64, len(ot.Partitions))
			for _, op := range ot.Partitions {
				if op.ErrorCode != 0 || op.Offset < 0 {
					continue // no committed offset on this partition
				}
				parts[int(op.Partition)] = op.Offset
			}
			if len(parts) > 0 {
				offs[ot.Topic] = parts
			}
		}
		offsetsByGroup[og.Group] = offs
	}

	groups := make([]Group, 0, len(dgResp.Groups))
	for _, dg := range dgResp.Groups {
		if dg.ProtocolType != "consumer" {
			continue
		}
		if dg.ErrorCode != 0 {
			warnings = append(warnings, fmt.Sprintf("group %s: describe groups error code %d; skipped", dg.Group, dg.ErrorCode))
			continue
		}
		group := Group{ID: dg.Group, State: dg.State, Offsets: offsetsByGroup[dg.Group]}
		for _, m := range dg.Members {
			group.Members = append(group.Members, GroupMember{ID: m.MemberID, ClientID: m.ClientID})
		}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
	return groups, warnings, nil
}
