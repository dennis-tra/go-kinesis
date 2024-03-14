package kinesis

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// Client is the interface that the [Producer] works with when communicating
// with AWS Kinesis. The interface defines the methods that are used. Operating
// on an interface allows injecting a mock implementation for ease of testing.
type Client interface {
	PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}
