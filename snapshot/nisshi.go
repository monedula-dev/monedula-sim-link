package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Nisshi (formerly Tansu) is a Kafka-protocol broker whose brokers keep no
// data of their own: a shared storage engine holds every record, offset and
// group, and every broker reports the same node ID, 111, in Metadata, as the
// leader and only replica of every partition (Nisshi's ARCHITECTURE.md; the
// constant NODE_ID in nisshi-broker). A snapshot therefore sees one broker
// whatever the cluster runs, and cannot tell which broker answered, so this
// module does not map a live Nisshi cluster: FetchWith refuses it with
// ErrNisshi and points at build_playground_url's nisshi-3b preset instead.

// NisshiNodeID is the node ID every Nisshi broker reports.
const NisshiNodeID = 111

// ErrNisshi is the error FetchWith returns for a cluster that answers like
// Nisshi. errors.Is matches it.
var ErrNisshi = errors.New("the cluster answers like Nisshi (formerly Tansu): Metadata lists one broker, node 111, and ApiVersions offers no CreatePartitions. " +
	"Every Nisshi broker reports itself as node 111, the leader and only replica of every partition, so a snapshot cannot count the brokers or tell which one answered, and monedula-sim-link does not map a live Nisshi cluster. " +
	"Build its link with the MCP tool build_playground_url instead: cluster nisshi-3b (simulator v2.3, not released yet), shape.brokers set to the broker count, and shape.nisshi.engine s3 for the S3 storage engine (PostgreSQL is the default)")

// nisshiCandidate reports whether a Metadata response's broker list is
// Nisshi's: exactly one broker, node 111. A Kafka cluster of one broker with
// node.id 111 has the same list, so nisshiAPIs decides.
func nisshiCandidate(brokers []kmsg.MetadataResponseBroker) bool {
	return len(brokers) == 1 && brokers[0].NodeID == NisshiNodeID
}

// nisshiAPIs reports whether an ApiVersions response is Nisshi's: a
// successful answer that offers no CreatePartitions (key 37). Nisshi does not
// implement CreatePartitions (its docs/compatibility.md); every Kafka broker
// since 1.0 does.
func nisshiAPIs(resp *kmsg.ApiVersionsResponse) bool {
	if resp == nil || resp.ErrorCode != 0 || len(resp.ApiKeys) == 0 {
		return false
	}
	createPartitions := (&kmsg.CreatePartitionsRequest{}).Key()
	for _, k := range resp.ApiKeys {
		if k.ApiKey == createPartitions {
			return false
		}
	}
	return true
}

// checkNisshi runs only when Metadata lists the single broker 111: it sends
// one ApiVersions request (a read-only handshake request that franz-go sends
// on every connection anyway) and returns ErrNisshi when the answer offers no
// CreatePartitions. A failed request is a warning, and the snapshot goes on.
func (c *Client) checkNisshi(ctx context.Context, brokers []kmsg.MetadataResponseBroker) (warning string, err error) {
	if !nisshiCandidate(brokers) {
		return "", nil
	}
	resp, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, c.kc)
	if err == nil && resp.ErrorCode != 0 {
		err = kerr.ErrorForCode(resp.ErrorCode)
	}
	if err != nil {
		return fmt.Sprintf("the cluster has one broker, node 111, as every Nisshi broker reports itself; the ApiVersions request that tells Nisshi from Kafka failed (%v), so it is mapped as a Kafka broker", err), nil
	}
	if nisshiAPIs(resp) {
		return "", ErrNisshi
	}
	return "", nil
}
