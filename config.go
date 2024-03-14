package kinesis

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Constants and default configuration taken from (2024-02-28):
// https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
const (
	// minPartitionKeyLength is the minimum partition key length. There must
	// be at least one character as the partition key. If no partition key
	// is needed, it is up to the user to generate a random one.
	minPartitionKeyLength = 1

	// maxPartitionKeyLength is the maximum partition key length.
	maxPartitionKeyLength = 256

	// Minimum number of concurrent writes to Kinesis.
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L592
	minConcurrency = 1

	// Maximum number of concurrent writes to Kinesis.
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L593
	maxConcurrency = 256

	// maxRecordSize is the maximum size of the data payload of a record before
	// base64-encoding.
	// Source: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
	maxRecordSize = 1e6 // 1 MB

	// maxShardBytesPerS is the maximum number of bytes that a shard can handle
	// per second. There is conflicting information. The AWS docs say it's 1MB
	// per second, and the AWS Console says it's 1MiB per second. Going for
	// the latter.
	maxShardBytesPerS = 1024 * 1024 // 1MiB / second

	// maxShardBytesPerS is the maximum number of records a shard can handle per
	// second.
	maxShardRecordsPerS = 1e3 // 1000 records / second

	// Maximum amount of data to send with a PutRecords request. Each record in
	// the request can be as large as 1 MB, up to a limit of 5 MB for the entire
	// request, including partition keys.
	// Source: https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
	maxCollectionSize = 5e6 // 5 MB

	// Minimum number of items to pack into a PutRecords request.
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L460
	minCollectionCount = 1

	// Maximum number of items to pack into a PutRecords request.
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L461
	maxCollectionCount = 500

	// Maximum number of bytes to pack into an aggregated Kinesis record. This
	// is the same as maxRecordSize
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L996
	maxAggregationSize = 1024 * 1024 // 1MiB

	// Minimum number of items to pack into an aggregated record.
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L401
	minAggregationCount = 1

	// Maximum number of items to pack into an aggregated record.
	// Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L402
	maxAggregationCount = math.MaxInt64
)

var (
	ErrProducerStopped     = fmt.Errorf("kinesis producer is stopped")
	ErrRecordSizeExceeded  = fmt.Errorf("data must be less than or equal to 1MB in size")
	ErrInvalidPartitionKey = fmt.Errorf("invalid parition key. Length must be at least 1 and at most 256")
)

// ProducerConfig holds all optional configuration parameters where no sane defaults
// can be guessed. The [NewProducer] signature lists required arguments.
// Everything else can be configured with this struct. Use [DefaultProducerConfig] to
// get a prepopulated struct which you can then adjust to your likings.
type ProducerConfig struct {
	// FlushInterval is a regular interval for flushing the buffer. Defaults to 5s.
	FlushInterval time.Duration

	// ShardUpdateInterval .
	ShardUpdateInterval time.Duration

	// CollectionMaxCount .
	CollectionMaxCount int

	// CollectionMaxSize .
	CollectionMaxSize int

	// AggregateMaxCount determine the maximum number of items to pack into an aggregated record.
	AggregateMaxCount int

	// AggregationMaxSize determine the maximum number of bytes to pack into an aggregated record. User records larger
	// than this will bypass aggregation.
	AggregateMaxSize int

	// How many parallel workers flush data
	Concurrency int

	// RetryLimit is the maximum number of retries for a single record to
	// be transmitted to Kinesis before the [Producer] passes it to the Notifiee
	// as undeliverable/dropped.
	RetryLimit int

	// Notifiee is a delegate implementation that gets notified in case the
	// Producer fails to transmit records or another error occurred.
	Notifiee Notifiee

	// Log is the logger to use in this library. By default, it's a logger
	// that doesn't print anything.
	Log *slog.Logger

	// Meter is an OpenTelemetry meter that go-kinesis uses to register metrics
	// with. By default, it uses a noop meter provider. Give it the default one
	// with otel.GetMeterProvider().
	Meter metric.Meter

	// clock is a field that's used for mocking the time
	clock clock.Clock
}

// DefaultProducerConfig returns the default [Producer] configuration.
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		FlushInterval:       5 * time.Second,
		ShardUpdateInterval: time.Minute,
		CollectionMaxCount:  maxCollectionCount, // Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L459
		CollectionMaxSize:   maxCollectionSize,  // Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L477
		AggregateMaxCount:   math.MaxUint32,     // Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L400
		AggregateMaxSize:    50 << 10,           // 50kib, Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L341
		Concurrency:         24,                 // Source: https://github.com/awslabs/amazon-kinesis-producer/blob/d63cb6213f2d6be8d04fdf84bc1c8a4eb7e695d2/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.java#L591
		RetryLimit:          3,                  // Arbitrarily set to 3
		Notifiee:            &NoopNotifiee{},
		clock:               clock.New(),
		Log:                 slog.New(slog.NewTextHandler(io.Discard, nil)), // /dev/null logger
		Meter:               noop.NewMeterProvider().Meter("go-kinesis"),
	}
}

// Validate checks the [Producer] configuration and returns an error if any
// field does not pass the validation. Use [DefaultProducerConfig] to get a valid
// configuration struct.
func (c *ProducerConfig) Validate() error {
	if c.CollectionMaxCount < minCollectionCount || c.CollectionMaxCount > maxCollectionCount {
		return fmt.Errorf("collection count max must be in range [%d,%d], got %d", minCollectionCount, maxCollectionCount, c.CollectionMaxCount)
	}

	if c.CollectionMaxSize < 1 || c.CollectionMaxSize > maxCollectionSize {
		return fmt.Errorf("collection max size must be in range [1,%.0f], got %d", maxCollectionSize, c.CollectionMaxSize)
	}

	if c.AggregateMaxCount < minAggregationCount || c.AggregateMaxCount > maxAggregationCount {
		return fmt.Errorf("aggregation max count must be in range [%d,%d], got %d", minAggregationCount, maxAggregationCount, c.AggregateMaxCount)
	}

	if c.AggregateMaxSize < 0 || c.AggregateMaxSize > maxAggregationSize {
		return fmt.Errorf("aggregation max size must be in range [0,%d], got %d", maxAggregationSize, c.AggregateMaxSize)
	}

	if c.Concurrency < minConcurrency || c.Concurrency > maxConcurrency {
		return fmt.Errorf("concurrency must be in range [%d,%d], got %d", minConcurrency, maxConcurrency, c.Concurrency)
	}

	if c.RetryLimit < 1 {
		return fmt.Errorf("retry limit must be greater than 0, got %d", c.RetryLimit)
	}

	if c.Notifiee == nil {
		return fmt.Errorf("notifiee must not be nil")
	}

	if c.Log == nil {
		return fmt.Errorf("logger must not be nil")
	}

	if c.Meter == nil {
		return fmt.Errorf("meter must not be nil")
	}

	return nil
}
