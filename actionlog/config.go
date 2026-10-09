package actionlog

import (
	"regexp"
	"strconv"
	"strings"
)

// The config_change whitelist (§6 of docs/playground-url-api.md). A value that
// is not on this list is dropped SILENTLY by the playground decoder, so the
// encoder refuses it up front. This is a faithful port of parseConfigValue in
// src/kafka-simulator/actionLog.ts — the full UI dispatch set, including the
// "cleared" (empty value segment) marker for the paths that support it.

// configRule describes one whitelisted path pattern: how to validate a value,
// whether the path accepts the cleared (empty value segment) marker, and a
// human-readable expectation used in error messages.
type configRule struct {
	re        *regexp.Regexp
	clearable bool
	expect    string
	accept    func(string) bool
}

// canonicalInt reports whether raw is a canonical decimal integer
// (String(parseInt(raw)) === raw) and returns its value.
func canonicalInt(raw string) (int, bool) {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	if strconv.Itoa(n) != raw {
		return 0, false
	}
	return n, true
}

func acceptIntMin(min int) func(string) bool {
	return func(v string) bool {
		n, ok := canonicalInt(v)
		return ok && n >= min
	}
}

func acceptAnyInt(v string) bool {
	_, ok := canonicalInt(v)
	return ok
}

func acceptEnum(vals ...string) func(string) bool {
	return func(v string) bool {
		for _, w := range vals {
			if v == w {
				return true
			}
		}
		return false
	}
}

var acceptBool = acceptEnum("true", "false")

// topicNameRe is the playground's sanitized-name alphabet, minus "." — the
// action-log grammar reserves "." for list items, so this codec keeps it out
// of topic names (see §7). The one exception is an add_group topic
// (validateGroupTopic), whose field is never split on ".".
var topicNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func acceptTopicName(v string) bool { return topicNameRe.MatchString(v) }

// acceptTopicList validates a group subscription: a non-empty "."-joined list
// of topic names (e.g. "orders.payments").
func acceptTopicList(v string) bool {
	if v == "" {
		return false
	}
	for _, t := range strings.Split(v, ".") {
		if !topicNameRe.MatchString(t) {
			return false
		}
	}
	return true
}

// acceptKeyValue: the fixed produce key may be any string, including empty —
// but the encoder cannot safely carry "~" (the one-way comma escape collapses
// it to ","), and ":" / "@" would corrupt the entry structure.
func acceptKeyValue(v string) bool { return !strings.ContainsAny(v, "~:@") }

const (
	expectTopicName = `a non-empty topic name (letters, digits, "_", "-")`
	expectBool      = `"true" or "false"`
)

// configRules is the exact §6 table, first match wins.
var configRules = []configRule{
	{re: regexp.MustCompile(`^producers\..+\.topic$`), expect: expectTopicName, accept: acceptTopicName},
	{re: regexp.MustCompile(`^producers\..+\.config\.acks$`), expect: `"0", "1" or "all"`, accept: acceptEnum("0", "1", "all")},
	{re: regexp.MustCompile(`^producers\..+\.config\.enableIdempotence$`), expect: expectBool, accept: acceptBool},
	{re: regexp.MustCompile(`^producers\..+\.config\.maxInFlight$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^producers\..+\.config\.retries$`), expect: "an integer ≥ 0", accept: acceptIntMin(0)},
	{re: regexp.MustCompile(`^producers\..+\.config\.retryBackoffMs$`), expect: "an integer ≥ 0", accept: acceptIntMin(0)},
	{re: regexp.MustCompile(`^producers\..+\.config\.lingerMs$`), expect: "an integer ≥ 0", accept: acceptIntMin(0)},
	{re: regexp.MustCompile(`^producers\..+\.config\.produceIntervalMs$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^producers\..+\.config\.partitioner$`), expect: `"default", "roundrobin" or "uniform-sticky"`, accept: acceptEnum("default", "roundrobin", "uniform-sticky")},
	{re: regexp.MustCompile(`^producers\..+\.config\.compressionType$`), expect: `"none", "gzip", "snappy", "lz4" or "zstd"`, accept: acceptEnum("none", "gzip", "snappy", "lz4", "zstd")},
	{re: regexp.MustCompile(`^producers\..+\.config\.quotaProducerByteRate$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^producers\..+\.config\.bufferMemory$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^producers\..+\.config\.maxBlockMs$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^producers\..+\.config\.batchSize$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^producers\..+\.config\.keyStrategy$`), expect: `"current", "empty" or "fixed"`, accept: acceptEnum("current", "empty", "fixed")},
	{re: regexp.MustCompile(`^producers\..+\.config\.keyValue$`), expect: `any string without "~", ":" or "@" (may be empty)`, accept: acceptKeyValue},
	{re: regexp.MustCompile(`^topics\..+\.minInSyncReplicas$`), expect: "an integer", accept: acceptAnyInt},
	// Partition COUNT (#560) — a positive integer (≥ 1), always set (NOT clearable);
	// the kafka-topics --alter --partitions N resize lowers to this config_change.
	{re: regexp.MustCompile(`^topics\..+\.partitions$`), expect: "an integer ≥ 1", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^topics\..+\.cleanupPolicy$`), expect: `"delete", "compact" or "compact,delete"`, accept: acceptEnum("delete", "compact", "compact,delete")},
	{re: regexp.MustCompile(`^topics\..+\.remoteStorageEnable$`), expect: expectBool, accept: acceptBool},
	// KIP-1163 draft `diskless.enable` (v2.0+; kafka-configs
	// --alter). Immutable after creation, so the engine always rejects a
	// change, but the entry still round-trips so that rejection re-appears
	// after a reload instead of silently vanishing (parity with parseConfigValue
	// in src/kafka-simulator/actionLog.ts).
	{re: regexp.MustCompile(`^topics\..+\.disklessEnable$`), expect: expectBool, accept: acceptBool},
	{re: regexp.MustCompile(`^topics\..+\.retentionMs$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^topics\..+\.retentionBytes$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^topics\..+\.localRetentionMs$`), clearable: true, expect: "an integer ≥ 1, or cleared", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^topic\..+\.unclean\.leader\.election\.enable$`), expect: expectBool, accept: acceptBool},
	{re: regexp.MustCompile(`^(consumer\.pollMs|group\..+\.consumer\.pollMs)$`), expect: "an integer", accept: acceptAnyInt},
	{re: regexp.MustCompile(`^(consumer\.autoOffsetReset|group\..+\.consumer\.autoOffsetReset)$`), expect: `"earliest", "latest" or "none"`, accept: acceptEnum("earliest", "latest", "none")},
	{re: regexp.MustCompile(`^(consumer\.isolationLevel|group\..+\.consumer\.isolationLevel)$`), expect: `"read_committed" or "read_uncommitted"`, accept: acceptEnum("read_committed", "read_uncommitted")},
	{re: regexp.MustCompile(`^group\..+\.partition\.assignment\.strategy$`), expect: `"range", "roundrobin", "sticky" or "cooperative-sticky"`, accept: acceptEnum("range", "roundrobin", "sticky", "cooperative-sticky")},
	{re: regexp.MustCompile(`^group\..+\.topics$`), expect: `a non-empty "."-joined topic list (e.g. "orders.payments")`, accept: acceptTopicList},
	{re: regexp.MustCompile(`^shareGroup\..+\.ackMode$`), expect: `"implicit" or "explicit"`, accept: acceptEnum("implicit", "explicit")},
	{re: regexp.MustCompile(`^shareGroup\..+\.acquireMode$`), expect: `"record_limit" or "batch_optimized"`, accept: acceptEnum("record_limit", "batch_optimized")},
	{re: regexp.MustCompile(`^shareGroup\..+\.renewAckEnable$`), expect: expectBool, accept: acceptBool},
	{re: regexp.MustCompile(`^shareGroup\..+\.deliveryLimit$`), expect: "an integer ≥ 1", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^shareGroup\..+\.maxPollRecords$`), expect: "an integer ≥ 1", accept: acceptIntMin(1)},
	{re: regexp.MustCompile(`^member\..+\.groupInstanceId$`), expect: expectBool, accept: acceptBool},
}

// lookupConfigRule returns the whitelist rule matching path, or nil when the
// path is not whitelisted at all.
func lookupConfigRule(path string) *configRule {
	for i := range configRules {
		if configRules[i].re.MatchString(path) {
			return &configRules[i]
		}
	}
	return nil
}
