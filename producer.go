//go:generate mockgen -source=kinesis.go -destination=client_test.go -package=kinesis -typed
package kinesis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
)

// Producer is TODO
type Producer struct {
	// reference the producer's configuration
	cfg *ProducerConfig

	// reference to the Kinesis client implementation
	client Client

	// the stream to transmit the records to
	streamName string

	// the list of currently active shards
	shards []*shard

	// sending records to this channel will forward them to the responsible
	// aggregator. There are individual aggregators for each shard.
	aggChan chan *dataRecord

	// the running total of the bytes that will be transmitted with the next
	// PutRecords request
	collSize int

	// all records that will be part of the next PutRecords request
	collBuf []*dataRecord

	// sending records to this channel will put them directly into the collBuf
	// for the next PutRecords request
	collChan chan *dataRecord

	// wait group that's done when all flush workers have returned
	flushWg sync.WaitGroup

	// channel on which flush jobs are sent that are picked up by the flush workers
	flushJobs chan *flushJob

	// channel on which flush results are returned. This is a buffered channel
	// with a size of the number of flush workers. That way sending flush jobs
	// and sending flush results won't interfere.
	flushResults chan *flushResult

	// flushTicker triggers a flush when no other limit that would usually
	// trigger a flush is exceeded.
	flushTicker *clock.Ticker

	// a channel on which the current list of active shards is sent
	shardResults chan []*shard

	// a channel that is closed when the Producer is ready to accept records
	readyChan chan struct{}

	// stopped is true when the Producer's Start context is cancelled
	stopped atomic.Bool

	// various metrics
	meterFlushCount metric.Int64Counter
	meterFlushBytes metric.Int64Counter
}

// NewProducer creates a new instance of Producer with the given parameters. It
// validates the client implementation, stream name, and configuration. It
// returns the new Producer instance or an error if validation or initialization
// fails.
//
// The client, stream name, and configuration parameters are required.
// Everything else should be configured using the [ProducerConfig] struct. Use
// [DefaultProducerConfig] as a starting point.
func NewProducer(client Client, streamName string, cfg *ProducerConfig) (*Producer, error) {
	// validate given client implementation
	if client == nil {
		return nil, fmt.Errorf("no client given")
	}

	// validate given stream name
	if streamName == "" {
		return nil, fmt.Errorf("empty stream name")
	} else if len(streamName) > 128 { // Source: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html#API_CreateStream_RequestSyntax
		return nil, fmt.Errorf("stream name too long, length %d", len(streamName))
	}

	// validate the given configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	cfg.Log = cfg.Log.With("scope", "go-kinesis")

	// initialize meters
	meterFlushCount, err := cfg.Meter.Int64Counter("flush_count", metric.WithDescription("The total number of flushed items"), metric.WithUnit("1"))
	if err != nil {
		return nil, fmt.Errorf("new flush_count counter: %w", err)
	}

	meterFlushBytes, err := cfg.Meter.Int64Counter("flush_bytes", metric.WithDescription("The total number of flushed records"), metric.WithUnit("B"))
	if err != nil {
		return nil, fmt.Errorf("new flush_count counter: %w", err)
	}

	// initialize producer
	p := &Producer{
		cfg:             cfg,
		client:          client,
		streamName:      streamName,
		shards:          make([]*shard, 0),
		aggChan:         make(chan *dataRecord),
		collSize:        0,
		collChan:        make(chan *dataRecord),
		collBuf:         make([]*dataRecord, 0, cfg.CollectionMaxCount),
		flushWg:         sync.WaitGroup{},
		flushJobs:       make(chan *flushJob),
		flushResults:    make(chan *flushResult, cfg.Concurrency), // buffered channel
		flushTicker:     cfg.clock.Ticker(cfg.FlushInterval),
		shardResults:    make(chan []*shard),
		readyChan:       make(chan struct{}),
		meterFlushCount: meterFlushCount,
		meterFlushBytes: meterFlushBytes,
	}

	return p, nil
}

// Start starts the [Producer] to accept records that will be flushed to the
// configured Kinesis Data Stream. However, first the Producer requests all
// existing shards. Call [Producer.Ready] to get notified when the Producer is
// actually ready to accept events.
func (p *Producer) Start(ctx context.Context) error {
	if p.stopped.Load() {
		return ErrProducerStopped
	}

	p.cfg.Log.Info("Kinesis producer started")

	// load all shards
	shards, err := p.listShards(ctx)
	if err != nil {
		close(p.readyChan)
		return err
	}
	p.shards = shards

	// start the workers to flush the records to Kinesis
	for i := 0; i < p.cfg.Concurrency; i++ {
		p.flushWg.Add(1)
		go p.flushWorker(ctx, fmt.Sprintf("%02d", i))
	}

	// start the shard watcher
	go p.watchShards(ctx)

	aggTicker := p.cfg.clock.Ticker(p.cfg.FlushInterval)

	p.cfg.Log.Info("Kinesis producer ready", "shards", len(p.shards))
	close(p.readyChan)

	for {
		select {
		case <-ctx.Done():
			p.shutdown()
			return nil
		case <-aggTicker.C:
			nextCollection := p.collectAggregators(ctx)
			aggTicker.Reset(nextCollection)
		case newShards := <-p.shardResults:
			p.handleNewShards(ctx, newShards)
		case <-p.flushTicker.C:
			p.flush(ctx, "interval")
		case result := <-p.flushResults:
			p.handleFlushResult(ctx, result)
		case record := <-p.collChan:
			p.collectRecord(ctx, record)
		case record := <-p.aggChan:
			p.aggRecord(ctx, record)
		}
	}
}

// Ready blocks until the [Producer] has requested all shards and is ready to
// accept records.
func (p *Producer) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.readyChan:
		return nil
	}
}

func (p *Producer) Put(ctx context.Context, partitionKey string, data []byte) error {
	rec, err := newDataRecord(partitionKey, nil, data)
	if err != nil {
		return fmt.Errorf("put record: %w", err)
	}

	return p.PutRecord(ctx, rec)
}

func (p *Producer) PutRecord(ctx context.Context, record Record) error {
	if p.stopped.Load() {
		return ErrProducerStopped
	}

	// verify that the payload is within the size limit
	if len(record.Data()) > maxRecordSize {
		return ErrRecordSizeExceeded
	}

	// verify that the partition key has the correct length. If no partition key
	// is needed, it is up to the user to generate a random one.
	if l := len(record.PartitionKey()); l < minPartitionKeyLength || l > maxPartitionKeyLength {
		return ErrInvalidPartitionKey
	}

	// construct new data record
	drecord, ok := record.(*dataRecord) // if PutRecord is called by Put, this is a *dataRecord
	if !ok {
		var err error
		drecord, err = newDataRecord(record.PartitionKey(), record.ExplicitHashKey(), record.Data())
		if err != nil {
			return fmt.Errorf("new data record: %w", err)
		}
	}

	// if the total size exceeds the configured [ProducerConfig.AggregateMaxSize] we
	// handle it as a simple Kinesis record and "collect" it instead.
	var recChan chan *dataRecord
	if drecord.Size() > p.cfg.AggregateMaxSize {
		recChan = p.collChan
	} else {
		recChan = p.aggChan
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case recChan <- drecord:
		return nil
	}
}

func (p *Producer) handleRetry(ctx context.Context, record *dataRecord) {
	if record.retries >= p.cfg.RetryLimit {
		// TODO: de-aggregate
		p.cfg.Notifiee.DroppedRecord(record)
	} else {
		p.collectRecord(ctx, record)
	}
}

func (p *Producer) shutdown() {
	p.stopped.Store(true)

	for range p.shardResults {
		// drain shardResults
	}

	close(p.flushJobs)
	p.flushWg.Wait()
	close(p.flushResults)

	for result := range p.flushResults {
		if result.err != nil {
			for _, rec := range result.job.records {
				p.cfg.Notifiee.DroppedRecord(rec)
			}
		} else if result.out != nil {
			for i, rec := range result.out.Records {
				if rec.ShardId != nil || rec.SequenceNumber != nil || rec.ErrorMessage == nil || rec.ErrorCode == nil {
					continue
				}
				p.cfg.Notifiee.DroppedRecord(result.job.records[i])
			}
		}
	}

	for _, rec := range p.collBuf {
		p.cfg.Notifiee.DroppedRecord(rec)
	}

	p.drainChan(p.aggChan)
	p.drainChan(p.collChan)

	close(p.aggChan)
	close(p.collChan)
}

// drainChan iterates over the given channel until its closed or there are no
// elements to consume anymore.
func (p *Producer) drainChan(c chan *dataRecord) {
	for {
		select {
		case rec, more := <-c:
			if more {
				p.cfg.Notifiee.DroppedRecord(rec)
			} else {
				return
			}
		default:
			return
		}
	}
}

func (p *Producer) watchShards(ctx context.Context) {
	p.cfg.Log.Debug("Started watching shards")
	defer p.cfg.Log.Debug("Stopped watching shards")

	defer close(p.shardResults)

	t := p.cfg.clock.Ticker(p.cfg.ShardUpdateInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// pass
		}

		newShards, err := p.listShards(ctx)
		if err != nil {
			p.cfg.Log.Warn("Failed listing shards", "err", err)
			continue
		}

		p.shardResults <- newShards
	}
}

func (p *Producer) listShards(ctx context.Context) ([]*shard, error) {
	p.cfg.Log.Debug("Listing shards...")

	var (
		shards []*shard
		next   *string
	)
	for {

		input := &kinesis.ListShardsInput{
			StreamName: aws.String(p.streamName),
			NextToken:  next,
		}
		resp, err := p.client.ListShards(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list shards: %w", err)
		}

		for _, respShard := range resp.Shards {
			// There may be many shards with overlapping hash key ranges due
			// to prior merge and split operations. The currently open shards
			// are the ones that do not have a
			// SequenceNumberRange.EndingSequenceNumber.
			if respShard.SequenceNumberRange != nil && respShard.SequenceNumberRange.EndingSequenceNumber != nil {
				continue
			}

			s, err := p.newShard(respShard)
			if err != nil {
				p.cfg.Log.Warn("Failed constructing shard struct", "err", err)
				continue
			}

			shards = append(shards, s)
		}

		next = resp.NextToken
		if next == nil {
			break
		}
	}

	// a sorted slice is assumed in the rest of the code.
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].firstHash.Cmp(shards[j].firstHash) == -1
	})

	return shards, nil
}

type shard struct {
	types.Shard
	shardID      string
	firstHash    *big.Int
	lastHash     *big.Int
	aggregator   *aggregator
	sizeLimiter  *rate.Limiter
	countLimiter *rate.Limiter
	nBytes       int
	count        int
}

func (p *Producer) newShard(s types.Shard) (*shard, error) {
	if s.HashKeyRange == nil {
		return nil, fmt.Errorf("hash key range not set")
	}
	hkRange := *s.HashKeyRange

	if hkRange.StartingHashKey == nil {
		return nil, fmt.Errorf("starting hash key range not set")
	}
	startingHashKey := *hkRange.StartingHashKey

	firstHash, ok := new(big.Int).SetString(startingHashKey, 10)
	if !ok {
		return nil, fmt.Errorf("convert starting hash key range to big int: %s", startingHashKey)
	}

	if hkRange.EndingHashKey == nil {
		return nil, fmt.Errorf("ending hash key range not set")
	}
	endingHashKey := *hkRange.EndingHashKey

	lastHash, ok := new(big.Int).SetString(endingHashKey, 10)
	if !ok {
		return nil, fmt.Errorf("convert ending hash key range to big int: %s", endingHashKey)
	}

	if s.ShardId == nil {
		return nil, fmt.Errorf("no shard ID")
	}

	return &shard{
		Shard:        s,
		shardID:      *s.ShardId,
		firstHash:    firstHash,
		lastHash:     lastHash,
		aggregator:   newAggregator(p.cfg.clock),
		sizeLimiter:  rate.NewLimiter(maxShardBytesPerS, maxShardBytesPerS),
		countLimiter: rate.NewLimiter(maxShardRecordsPerS, maxShardRecordsPerS),
		count:        0,
		nBytes:       0,
	}, nil
}

type flushJob struct {
	collSize int
	flushAt  time.Time
	records  []*dataRecord
}

type flushResult struct {
	id  string
	out *kinesis.PutRecordsOutput
	err error
	job *flushJob
}

func (p *Producer) flushWorker(ctx context.Context, workerID string) {
	defer p.flushWg.Done()

	logger := p.cfg.Log.With(slog.String("workerID", workerID))
	logger.Debug("Started flush worker")
	defer logger.Debug("Stopped flush worker")

	for job := range p.flushJobs {
		// prepare request data structures
		putRecords := make([]types.PutRecordsRequestEntry, len(job.records))
		for i, rec := range job.records {
			putRecords[i] = types.PutRecordsRequestEntry{
				Data:            rec.data,
				PartitionKey:    aws.String(rec.PartitionKey()),
				ExplicitHashKey: rec.ExplicitHashKey(),
			}
		}

		input := &kinesis.PutRecordsInput{
			StreamName: aws.String(p.streamName),
			Records:    putRecords,
		}

		select {
		case <-ctx.Done():
			p.flushResults <- &flushResult{
				id:  workerID,
				out: nil,
				err: ctx.Err(),
				job: job,
			}
			return
		case <-p.cfg.clock.After(p.cfg.clock.Until(job.flushAt)):
			// pass
		}

		// do the request
		p.meterFlushCount.Add(ctx, int64(len(input.Records)), metric.WithAttributes(attribute.String("type", "sent")))
		p.meterFlushBytes.Add(ctx, int64(job.collSize))
		logger.Info("--> Flush records", "count", len(input.Records), "size", fmtSize(job.collSize))
		out, err := p.client.PutRecords(ctx, input)
		logger.Info("<-- Flushed records", "count", len(input.Records), "size", fmtSize(job.collSize))

		p.flushResults <- &flushResult{
			id:  workerID,
			out: out,
			err: err,
			job: job,
		}
	}
}

func (p *Producer) flush(ctx context.Context, reason string) {
	if p.collSize == 0 {
		return
	}

	p.cfg.Log.Debug("Schedule flush", "count", len(p.collBuf), "size", fmtSize(p.collSize), "reason", reason)

	flushAt := p.cfg.clock.Now()
	for i := range p.shards {
		s := p.shards[i]

		res := s.sizeLimiter.ReserveN(p.cfg.clock.Now(), s.nBytes)
		if p.cfg.clock.Now().Add(res.Delay()).After(flushAt) {
			flushAt = p.cfg.clock.Now().Add(res.Delay())
		}

		res = s.countLimiter.ReserveN(p.cfg.clock.Now(), s.count)
		if p.cfg.clock.Now().Add(res.Delay()).After(flushAt) {
			flushAt = p.cfg.clock.Now().Add(res.Delay())
		}

		s.nBytes = 0
		s.count = 0
	}

	job := &flushJob{
		collSize: p.collSize,
		records:  p.collBuf,
		flushAt:  flushAt,
	}

	select {
	case <-ctx.Done():
		for _, rec := range job.records {
			p.cfg.Notifiee.DroppedRecord(rec)
		}
		return
	case p.flushJobs <- job:
		// flushed job
	}

	p.collBuf = nil
	p.collSize = 0

	p.flushTicker.Reset(p.cfg.FlushInterval)
}

func (p *Producer) collectRecord(ctx context.Context, record *dataRecord) {
	recordSize := record.Size()
	shardIdx := p.shardIdx(record.hk)

	if p.shards[shardIdx].nBytes+recordSize > maxShardBytesPerS {
		p.flush(ctx, "shard_size")
	}

	// check if the existing collected records plus the new record would exceed
	// the total size of max size limit. If so, we flush the existing records.
	if p.collSize+recordSize > p.cfg.CollectionMaxSize {
		p.flush(ctx, "coll_size")
	}

	// do some accounting and store the new record in our buffer
	p.collSize += recordSize
	p.collBuf = append(p.collBuf, record)
	p.shards[shardIdx].nBytes += recordSize
	p.shards[shardIdx].count += 1

	// check if the number of buffered records is equal to the maximum of
	// allowed records per request.
	if len(p.collBuf) >= p.cfg.CollectionMaxCount {
		p.flush(ctx, "coll_count")
	}

	if p.shards[shardIdx].count == maxShardRecordsPerS {
		p.flush(ctx, "shard_count")
	}
}

func (p *Producer) collectAggregators(ctx context.Context) time.Duration {
	nextCollection := p.cfg.FlushInterval

	for _, s := range p.shards {
		// if the last drain is longer ago than the FlushInterval -> drain it
		if p.cfg.clock.Since(s.aggregator.lastDrain) > p.cfg.FlushInterval {
			aggRec, err := s.aggregator.drain()
			if err != nil {
				if !errors.Is(err, ErrNoRecordsToDrain) {
					p.cfg.Log.Warn("Failed draining aggregator", "err", err)
				}
			} else {
				p.collectRecord(ctx, aggRec)
			}
		} else if p.cfg.clock.Until(s.aggregator.lastDrain) > 0 {
			// if the earliest next drain is in the future, keep track of it
			nextCollection = p.cfg.clock.Until(s.aggregator.lastDrain)
		}
	}

	return nextCollection
}

func (p *Producer) aggRecord(ctx context.Context, record *dataRecord) {
	idx := p.shardIdx(record.hk)

	// we couldn't find the responsible aggregator. This should never happen.
	// However, if it does, log an error and collect the record regularly.
	if idx == -1 {
		p.cfg.Log.Warn("Failed finding aggregator for hash key", "hk", record.hk.String())
		p.collectRecord(ctx, record)
		return
	}

	// get a reference of that aggregator
	agg := p.shards[idx].aggregator

	// Check if the new record would exceed the allowed maximum size of an
	// aggregated record (if added).
	if agg.sizeWith(record) > p.cfg.AggregateMaxSize {
		// the aggregator is at its capacity, so drain it and collect the
		// resulting record.
		aggRec, err := agg.drain()
		if err != nil {
			p.cfg.Log.Warn("Failed draining aggregator", "err", err)
		} else {
			p.collectRecord(ctx, aggRec)
		}
	}

	// the aggregator has the capacity to carry the new record -> add it.
	agg.put(record)

	// check if the aggregator carries the maximum number of records, if so -> drain it
	if agg.count() == p.cfg.AggregateMaxCount {
		aggRec, err := agg.drain()
		if err != nil {
			p.cfg.Log.Warn("Failed draining aggregator", "err", err)
		} else {
			p.collectRecord(ctx, aggRec)
		}
	}
}

// shardIdx performs a binary-search to find the responsible shard index. It
// uses the [Producer.aggregators] slice because there is one aggregator per shard.
// We might be able to use some modulo arithmetic for O(1) access. However,
// this would assume equal hash key range sizes per shard which I don't know
// if it's true.
func (p *Producer) shardIdx(hashKey *big.Int) int {
	return sort.Search(len(p.shards), func(i int) bool {
		return p.shards[i].lastHash.Cmp(hashKey) >= 0
	})
}

func (p *Producer) handleFlushResult(ctx context.Context, result *flushResult) {
	p.cfg.Log.Info("X Handle Flush result", "success", result.out != nil)
	if result.err != nil {
		p.cfg.Log.Warn("Failed sending records to Kinesis", "err", result.err.Error())
		p.meterFlushCount.Add(ctx, int64(len(result.job.records)), metric.WithAttributes(attribute.String("type", "failed")))

		// add all records back to the queue
		for _, retryRec := range result.job.records {
			retryRec.retries += 1
			p.handleRetry(ctx, retryRec)
		}

		return
	}

	// if we don't have something to work with or if we don't have any
	// failed records, continue to read a new batch
	if result.out == nil || result.out.FailedRecordCount == nil || *result.out.FailedRecordCount == 0 {
		return
	}

	p.cfg.Log.Warn("Flush partial failure", "workerID", result.id, "failed", *result.out.FailedRecordCount, "total", len(result.out.Records))
	p.meterFlushCount.Add(ctx, int64(*result.out.FailedRecordCount), metric.WithAttributes(attribute.String("type", "failed")))

	// we have failed records, search for them in the result set and put
	// them in the retry queue.

	for i, rec := range result.out.Records {
		if rec.ShardId != nil || rec.SequenceNumber != nil || rec.ErrorMessage == nil || rec.ErrorCode == nil {
			continue
		}
		retryRec := result.job.records[i]
		retryRec.retries += 1

		p.handleRetry(ctx, retryRec)
	}
}

func (p *Producer) handleNewShards(ctx context.Context, newShards []*shard) {
	// check if the list of shards has changed
	shardsEqual := true

	// if the lengths are not equal, they definitely have changed
	if len(p.shards) != len(newShards) {
		shardsEqual = false
	} else {
		// we have an equal number of shards, check if the hash key ranges are the same
		for i, newShard := range newShards {
			// save slice access because of equal length
			oldShard := p.shards[i]

			// compare hash key ranges
			if newShard.firstHash.Cmp(oldShard.firstHash) == 0 && newShard.lastHash.Cmp(oldShard.lastHash) == 0 {
				continue
			}

			// hash key ranges differ -> the shard lists are not equal -> stop here
			shardsEqual = false
			break
		}
	}

	// if the shards are equal we don't do anything
	if shardsEqual {
		return
	}

	p.cfg.Log.Info("Observed resharding", "newShards", len(newShards))

	// some shards may still be the same, in that case we want to preserve
	// the limits.
	// TODO: This is an O(n^2) loop and should be optimized.
	for _, newShard := range newShards {
		for _, oldShard := range p.shards {
			if oldShard.shardID != newShard.shardID || newShard.firstHash.Cmp(oldShard.firstHash) != 0 || newShard.lastHash.Cmp(oldShard.lastHash) != 0 {
				continue
			}

			newShard.sizeLimiter = oldShard.sizeLimiter
			newShard.countLimiter = oldShard.countLimiter

			break
		}
	}

	// replace the Producer's shards and take the records of the old shard's
	// aggregators and aggregate them again with the new set of shards.
	oldShards := p.shards
	p.shards = newShards

	for _, shard := range oldShards {
		for _, rec := range shard.aggregator.buf {
			p.aggRecord(ctx, rec)
		}
	}
}

func fmtSize(n int) string {
	return fmt.Sprintf("%.3fMB", float64(n)/1e6)
}
