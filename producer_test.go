package kinesis

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
)

// Create a 128bit integer
var maxInt128, _ = new(big.Int).SetString("340282366920938463463374607431768211455", 10) // 2^128 - 1

// assertClosed triggers a test failure if the given channel was not closed but
// carried more values or a timeout occurs (given by the context).
func assertClosed[T any](t testing.TB, ctx context.Context, c <-chan T) {
	t.Helper()

	select {
	case _, more := <-c:
		assert.False(t, more)
	case <-ctx.Done():
		t.Fatal("timeout closing channel")
	}
}

func testCtx(t *testing.T) context.Context {
	t.Helper()

	timeout := 10 * time.Minute
	goal := time.Now().Add(timeout)

	deadline, ok := t.Deadline()
	if !ok {
		deadline = goal
	} else {
		deadline = deadline.Add(-time.Second)
		if deadline.After(goal) {
			deadline = goal
		}
	}

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	t.Cleanup(cancel)
	return ctx
}

func testConfig() (*ProducerConfig, *clock.Mock) {
	cfg := DefaultProducerConfig()
	clk := clock.NewMock()
	cfg.clock = clk
	return cfg, clk
}

// startProducer starts the kinesis producer, waits until it's actually ready,
// and registers a cleanup function that will properly shut down the producer at
// the end of the test. Being ready means that the listShards API was made and
// that the infinite loop is reached.
func startProducer(t *testing.T, ctx context.Context, p *Producer) {
	t.Helper()

	// configure mock calls
	p.client.(*MockClient).EXPECT().ListShards(gomock.Any(), gomock.Any()).Times(1).Return(singleShardResponse(), nil)

	// start the producer
	producerCtx, stopProducer := context.WithCancel(ctx)
	go func() {
		err := p.Start(producerCtx)
		require.NoError(t, err)
	}()

	t.Cleanup(func() {
		stopProducer()
		assertClosed(t, ctx, p.stopped)
		goleak.VerifyNone(t)
	})
	// wait until idle
	assert.NoError(t, p.WaitIdle(ctx))
}

func mustNewDataRecord(t *testing.T, partitionKey string, data []byte) *dataRecord {
	rec, err := newDataRecord(partitionKey, nil, data)
	require.NoError(t, err)
	return rec
}

func singleShardResponse() *kinesis.ListShardsOutput {
	return &kinesis.ListShardsOutput{
		Shards: []types.Shard{
			{
				HashKeyRange: &types.HashKeyRange{
					StartingHashKey: aws.String(big.NewInt(0).String()),
					EndingHashKey:   aws.String(maxInt128.String()),
				},
				ShardId: aws.String("shard-0"),
			},
		},
	}
}

func TestProducer_NewProducer(t *testing.T) {
	client := NewMockClient(gomock.NewController(t))
	streamName := "test-stream"
	cfg := DefaultProducerConfig()
	p, err := NewProducer(client, streamName, cfg)
	require.NoError(t, err)

	assert.Equal(t, cfg, p.cfg)
	assert.Equal(t, streamName, p.streamName)
	assert.Equal(t, client, p.client)
	assert.NotNil(t, p.shards)
	assert.NotNil(t, p.aggChan)
	assert.Zero(t, p.collSize)
	assert.NotNil(t, p.collChan)
	assert.NotNil(t, p.collBuf)
	assert.NotNil(t, p.flushJobs)
	assert.NotNil(t, p.flushResults)
	assert.NotNil(t, p.flushTicker)
	assert.NotNil(t, p.idleWaiters)
	assert.NotNil(t, p.newIdleWaiter)
	assert.Equal(t, int32(producerStateUnstarted), p.state.Load())
	assert.NotNil(t, p.stopped)
	assert.NotNil(t, p.meterFlushCount)
	assert.NotNil(t, p.meterFlushBytes)
}

func TestProducer_Start_stop(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", DefaultProducerConfig())
	require.NoError(t, err)

	startProducer(t, ctx, p)
}

func TestProducer_WaitIdle(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	cfg := DefaultProducerConfig()

	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)

	// configure mock calls
	client.EXPECT().ListShards(gomock.Any(), gomock.Any()).Times(1).Return(singleShardResponse(), nil)

	producerCtx, stopProducer := context.WithCancel(ctx)
	go func() {
		err := p.Start(producerCtx)
		require.NoError(t, err)
	}()

	err = p.WaitIdle(ctx)
	require.NoError(t, err)

	stopProducer()

	assert.NoError(t, p.WaitStopped(ctx))

	err = p.Put(ctx, "A", nil)
	assert.ErrorIs(t, err, ErrProducerStopped)
}

func TestProducer_NewProducer_validation(t *testing.T) {
	client := NewMockClient(gomock.NewController(t))
	streamName := "test-stream"

	t.Run("no_client", func(t *testing.T) {
		_, err := NewProducer(nil, streamName, DefaultProducerConfig())
		assert.Error(t, err)
	})

	t.Run("no_stream", func(t *testing.T) {
		_, err := NewProducer(client, "", DefaultProducerConfig())
		assert.Error(t, err)
	})

	t.Run("invalid_stream_name", func(t *testing.T) {
		_, err := NewProducer(client, strings.Repeat("A", 128), DefaultProducerConfig())
		assert.NoError(t, err)
		_, err = NewProducer(client, strings.Repeat("A", 129), DefaultProducerConfig())
		assert.Error(t, err)
	})

	t.Run("invalid_config", func(t *testing.T) {
		cfg := DefaultProducerConfig()
		cfg.Notifiee = nil // make invalid
		_, err := NewProducer(client, streamName, cfg)
		assert.Error(t, err)
	})
}

func TestProducer_Put_inputValidation(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", DefaultProducerConfig())
	require.NoError(t, err)

	startProducer(t, ctx, p)

	t.Run("max_record_size_exceeded", func(t *testing.T) {
		data := make([]byte, maxRecordSize+1)
		err := p.Put(ctx, "partition-key", data)
		assert.ErrorIs(t, ErrRecordSizeExceeded, err)
	})

	t.Run("no_partition_key", func(t *testing.T) {
		err := p.Put(ctx, "", make([]byte, 1))
		assert.ErrorIs(t, ErrInvalidPartitionKey, err)
	})

	t.Run("too_long_partition_key", func(t *testing.T) {
		key := strings.Repeat("X", maxPartitionKeyLength+1)
		err := p.Put(ctx, key, make([]byte, 1))
		assert.ErrorIs(t, ErrInvalidPartitionKey, err)
	})
}

func TestProducer_Put_collectRecord_flush(t *testing.T) {
	// init context, configuration, mock, and producer
	ctx := testCtx(t)
	cfg, clk := testConfig()
	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)

	// define test data
	testRecords := []*dataRecord{
		mustNewDataRecord(t, "partition-key-1", []byte("data-1")),
		mustNewDataRecord(t, "partition-key-2", []byte("data-2")),
		mustNewDataRecord(t, "partition-key-3", []byte("data-3")),
	}

	done := make(chan struct{})
	client.EXPECT().PutRecords(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
		assert.Equal(t, p.streamName, *params.StreamName)
		assert.Len(t, params.Records, len(testRecords))

		for i, rec := range testRecords {
			assert.Equal(t, rec.pk, *params.Records[i].PartitionKey)
			assert.Equal(t, rec.data, params.Records[i].Data)
		}

		close(done)

		return &kinesis.PutRecordsOutput{}, nil
	})

	// start the producer
	startProducer(t, ctx, p)

	// there should be one shard
	assert.Len(t, p.shards, 1)

	// simulate putting record to the producer
	for _, rec := range testRecords {
		prevCollBufLen := len(p.collBuf)
		prevCollSize := p.collSize

		p.collectRecord(ctx, rec)

		// assert accounting
		assert.Len(t, p.collBuf, prevCollBufLen+1)
		assert.Equal(t, p.collSize, prevCollSize+rec.Size())
		assert.Equal(t, p.shards[0].size, prevCollSize+rec.Size())
		assert.Equal(t, p.shards[0].count, prevCollBufLen+1)
	}

	// increase the time by cfg.FlushInterval to trigger a flush
	clk.Add(cfg.FlushInterval)

	// wait until the PutRecords call on our client was called
	assertClosed(t, ctx, done)

	// expect that the producer has reset its internal data
	assert.Equal(t, 0, p.collSize)
	assert.Empty(t, p.collBuf)
}

func TestProducer_Put_collectRecord_collectionMaxSizeExceeded(t *testing.T) {
	// init context, configuration, mock, and producer
	ctx := testCtx(t)

	cfg := DefaultProducerConfig()
	cfg.CollectionMaxSize = 10
	cfg.AggregateMaxSize = 0

	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)

	// start the producer run loop
	startProducer(t, ctx, p)

	// add 5 records with a size of 2 bytes to the producer. The maximum size
	// is set to 10.
	for i := 0; i < 5; i++ {
		err := p.PutRecord(ctx, mustNewDataRecord(t, "1", []byte("a")))
		require.NoError(t, err)
	}
	runtime.Gosched()

	done := make(chan struct{})
	client.EXPECT().PutRecords(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
		close(done)
		return &kinesis.PutRecordsOutput{}, nil
	})

	err = p.PutRecord(ctx, mustNewDataRecord(t, "1", []byte("a")))
	require.NoError(t, err)

	// wait until the PutRecords call on our client was called
	assertClosed(t, ctx, done)
}

func TestProducer_Put_collectRecord_collectionMaxCountExceeded(t *testing.T) {
	// init context, configuration, mock, and producer
	ctx := testCtx(t)

	cfg := DefaultProducerConfig()
	cfg.CollectionMaxCount = 2
	cfg.AggregateMaxSize = 0

	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)

	// start the producer run loop
	startProducer(t, ctx, p)

	err = p.Put(ctx, "1", []byte("a"))
	require.NoError(t, err)

	done := make(chan struct{})
	client.EXPECT().PutRecords(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
		close(done)
		return &kinesis.PutRecordsOutput{Records: make([]types.PutRecordsResultEntry, 0)}, nil
	})

	err = p.Put(ctx, "1", []byte("a"))
	require.NoError(t, err)

	// wait until the PutRecords call on our client was called
	assertClosed(t, ctx, done)
}

func TestProducer_listShards(t *testing.T) {
	// init context, configuration, mock, and producer
	ctx := testCtx(t)

	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", DefaultProducerConfig())
	require.NoError(t, err)

	// The following code generates Shard Stubs. We create four mockShards that
	// cover non-overlapping equal hash key ranges. The hash space goes until
	// the maximum of a md5 hash (2^128).

	shardCount := int64(4)

	// calculate the size of each hash key range
	rangeSize := new(big.Int).Div(maxInt128, big.NewInt(shardCount))

	// calculate the ranges.
	mockShards := make([]types.Shard, shardCount)
	for i := range mockShards {

		// start value of range
		start := new(big.Int).Mul(rangeSize, big.NewInt(int64(i)))

		// end value of range
		end := new(big.Int).Sub(new(big.Int).Mul(rangeSize, big.NewInt(int64(i+1))), big.NewInt(1))

		mockShards[i] = types.Shard{
			HashKeyRange: &types.HashKeyRange{
				StartingHashKey: aws.String(start.String()),
				EndingHashKey:   aws.String(end.String()),
			},
			SequenceNumberRange: &types.SequenceNumberRange{},
		}
	}

	// configure mock calls
	out := &kinesis.ListShardsOutput{Shards: mockShards}
	client.EXPECT().ListShards(gomock.Any(), gomock.Any()).Times(1).Return(out, nil)

	shards, err := p.listShards(ctx)
	assert.NoError(t, err)

	for i, s := range shards {
		start, ok := new(big.Int).SetString(*mockShards[i].HashKeyRange.StartingHashKey, 10)
		require.True(t, ok)

		assert.Zero(t, start.Cmp(s.firstHash))

		end, ok := new(big.Int).SetString(*mockShards[i].HashKeyRange.EndingHashKey, 10)
		require.True(t, ok)
		assert.Zero(t, end.Cmp(s.lastHash))

		assert.Equal(t, mockShards[i], s.Shard)
		assert.NotNil(t, s.sizeLimiter)
		assert.NotNil(t, s.countLimiter)
		assert.NotNil(t, s.aggregator.buf)
		assert.NotNil(t, s.aggregator.keys)
		assert.NotNil(t, s.aggregator.keysIdx)
		assert.False(t, s.aggregator.lastDrain.IsZero())
	}
}

func TestProducer_notifiee_droppedRecords_collRec(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	cfg, clk := testConfig()
	cfg.AggregateMaxSize = 0

	dropped := make(chan Record)
	cfg.Notifiee = &NotifieeBundle{
		DroppedRecordF: func(record Record) {
			dropped <- record
			close(dropped)
		},
	}
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)
	testErr := fmt.Errorf("test error")

	var wg sync.WaitGroup
	client.EXPECT().PutRecords(gomock.Any(), gomock.Any()).Times(cfg.RetryLimit).DoAndReturn(func(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
		wg.Done()
		return nil, testErr
	})

	// start the producer run loop
	startProducer(t, ctx, p)

	testRec := &testRecord{
		dataRecord: mustNewDataRecord(t, "1", []byte("a")),
		payload:    "payload",
	}

	err = p.PutRecord(ctx, testRec)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		clk.Add(cfg.FlushInterval)
		wg.Wait()
	}

	rec := <-dropped
	require.NotNil(t, rec)
	got, ok := rec.(*testRecord)
	assert.True(t, ok)
	assert.Equal(t, testRec.payload, got.payload)
}

func TestProducer_notifiee_droppedRecords_aggRec(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	cfg, clk := testConfig()

	dropped := make(chan Record)
	cfg.Notifiee = &NotifieeBundle{
		DroppedRecordF: func(record Record) {
			dropped <- record
			close(dropped)
		},
	}
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)
	testErr := fmt.Errorf("test error")

	var wg sync.WaitGroup
	client.EXPECT().PutRecords(gomock.Any(), gomock.Any()).Times(cfg.RetryLimit).DoAndReturn(func(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
		wg.Done()
		return nil, testErr
	})

	// start the producer run loop
	startProducer(t, ctx, p)

	testRec := &testRecord{
		dataRecord: mustNewDataRecord(t, "1", []byte("a")),
		payload:    "payload",
	}

	err = p.PutRecord(ctx, testRec)
	require.NoError(t, err)

	clk.Add(cfg.FlushInterval)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		clk.Add(cfg.FlushInterval)
		wg.Wait()
	}

	rec := <-dropped
	require.NotNil(t, rec)
	got, ok := rec.(*testRecord)
	assert.True(t, ok)
	assert.Equal(t, testRec.payload, got.payload)
}

func TestProducer_listShards_sortsShards(t *testing.T) {
	// init context, configuration, mock, and producer
	ctx := testCtx(t)

	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", DefaultProducerConfig())
	require.NoError(t, err)

	// out of order shards
	mockShards := []types.Shard{
		{
			ShardId: aws.String("shard-1"),
			HashKeyRange: &types.HashKeyRange{
				StartingHashKey: aws.String("2"),
				EndingHashKey:   aws.String("3"),
			},
			SequenceNumberRange: &types.SequenceNumberRange{},
		},
		{
			ShardId: aws.String("shard-0"),
			HashKeyRange: &types.HashKeyRange{
				StartingHashKey: aws.String("0"),
				EndingHashKey:   aws.String("1"),
			},
			SequenceNumberRange: &types.SequenceNumberRange{},
		},
	}
	out := &kinesis.ListShardsOutput{Shards: mockShards}
	client.EXPECT().ListShards(gomock.Any(), gomock.Any()).Times(1).Return(out, nil)

	shards, err := p.listShards(ctx)
	assert.NoError(t, err)

	assert.Len(t, shards, 2)

	assert.EqualValues(t, 0, shards[0].firstHash.Uint64())
	assert.EqualValues(t, 1, shards[0].lastHash.Uint64())
	assert.EqualValues(t, 2, shards[1].firstHash.Uint64())
	assert.EqualValues(t, 3, shards[1].lastHash.Uint64())
}

func TestProducer_initAggregators_listShardsFails(t *testing.T) {
	// init context, configuration, mock, and producer
	ctx := testCtx(t)

	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", DefaultProducerConfig())
	require.NoError(t, err)

	testErr := fmt.Errorf("test error")
	client.EXPECT().ListShards(gomock.Any(), gomock.Any()).Times(1).Return(nil, testErr)

	_, err = p.listShards(ctx)
	assert.ErrorIs(t, err, testErr)
}

func TestProducer_aggRecord_binarySearch(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	p, err := NewProducer(client, "test-stream", DefaultProducerConfig())
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		p.shards = append(p.shards, &shard{
			aggregator: newAggregator(clock.New()),
			firstHash:  big.NewInt(int64(10 * i)),
			lastHash:   big.NewInt(int64((10 * i) + 9)),
		})
	}

	tests := []struct {
		hk     int // hash key
		aggIdx int // expected responsible aggregator
	}{
		{hk: 15, aggIdx: 1},
		{hk: 0, aggIdx: 0},
		{hk: 9, aggIdx: 0},
		{hk: 20, aggIdx: 2},
		{hk: 29, aggIdx: 2},
		{hk: 39, aggIdx: 3},
	}

	for _, tt := range tests {
		rec := &dataRecord{
			pk:   "partition-key",
			hk:   big.NewInt(int64(tt.hk)),
			data: []byte("data"),
		}

		agg := p.shards[tt.aggIdx].aggregator

		newSize := agg.sizeWith(rec)
		newCount := agg.count() + 1

		p.aggRecord(ctx, rec)

		assert.Equal(t, newSize, agg.aggSize)
		assert.Len(t, agg.buf, newCount)
	}
}

func TestProducer_aggRecord_aggregateMaxSize(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	cfg := DefaultProducerConfig()
	cfg.AggregateMaxSize = 100
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)

	p.shards = []*shard{{
		aggregator: newAggregator(clock.New()),
		firstHash:  big.NewInt(int64(0)),
		lastHash:   maxInt128,
	}}

	rec := mustNewDataRecord(t, "partition-key", []byte("data"))

	// adding 6 of the above records have a combined aggregated size of 95 bytes.
	// adding a seventh one will exceed the AggregateMaxSize of 100 bytes
	for i := 0; i < 6; i++ {
		p.aggRecord(ctx, rec)
	}

	assert.Equal(t, 0, p.collSize)
	assert.Len(t, p.collBuf, 0)

	// because the new record will exceed the limit, the aggregator will emit
	// all six records it got so far and add the seventh one to itself.
	p.aggRecord(ctx, rec)

	// assert that the aggregated record was collected
	assert.Equal(t, 108, p.collSize)
	assert.Len(t, p.collBuf, 1)

	// assert that the aggregator has reset itself and added the latest record
	assert.Len(t, p.shards[0].aggregator.buf, 1)
}

func TestProducer_aggRecord_aggregateMaxCount(t *testing.T) {
	ctx := testCtx(t)
	client := NewMockClient(gomock.NewController(t))
	cfg := DefaultProducerConfig()
	cfg.AggregateMaxCount = 10
	p, err := NewProducer(client, "test-stream", cfg)
	require.NoError(t, err)

	p.shards = []*shard{{
		aggregator: newAggregator(clock.New()),
		firstHash:  big.NewInt(int64(0)),
		lastHash:   maxInt128,
	}}

	rec := mustNewDataRecord(t, "partition-key", []byte("data"))

	// adding 9 of the above records won't exceed the aggregate max count
	for i := 0; i < 9; i++ {
		p.aggRecord(ctx, rec)
	}

	assert.Equal(t, 0, p.collSize)
	assert.Len(t, p.collBuf, 0)

	// because the new record will exceed the count limit, the aggregator will
	// emit all 10 records.
	p.aggRecord(ctx, rec)

	// assert that the aggregated record was collected
	assert.Equal(t, 148, p.collSize)
	assert.Len(t, p.collBuf, 1)

	// assert that the aggregator has reset itself and added the latest record
	assert.Len(t, p.shards[0].aggregator.buf, 0)
}

func TestProducer_handleNewShards(t *testing.T) {
	t.Skipf("TODO")
}
