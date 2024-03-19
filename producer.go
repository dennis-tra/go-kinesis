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

type producerState int32

const (
	producerStateUnstarted producerState = 0
	producerStateStarting  producerState = 1
	producerStateStarted   producerState = 2
	producerStateStopping  producerState = 3
	producerStateStopped   producerState = 4
)

func (p producerState) String() string {
	switch p {
	case producerStateUnstarted:
		return "unstarted"
	case producerStateStarting:
		return "starting"
	case producerStateStarted:
		return "started"
	case producerStateStopping:
		return "stopping"
	case producerStateStopped:
		return "stopped"
	default:
		return fmt.Sprintf("unknown state %d", p)
	}
}

// Producer is batches records for efficient transmission to a Kinesis Data Stream.
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

	// aggTicker triggers draining shard aggregator
	aggTicker *clock.Ticker

	// a channel on which the current list of active shards is sent
	shardResults chan []*shard

	// a channel on which new waiters will be transmitted
	newIdleWaiter chan chan struct{}

	// a list of waiters that wait for the [Producer] to become idle
	idleWaiters []chan struct{}

	// the number of inflight records
	inFlightRecords int

	// state is true when the Producer's Start context is cancelled
	state *atomic.Int32

	// stopped is a channel that will be closed when the Producer has stopped
	stopped chan struct{}

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

	// add a log message indicator that it originates from this library
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

	var state atomic.Int32
	state.Store(int32(producerStateUnstarted))

	// initialize producer
	p := &Producer{
		cfg:             cfg,
		client:          client,
		streamName:      streamName,
		shards:          make([]*shard, 0),
		aggChan:         make(chan *dataRecord),
		collSize:        0,
		collBuf:         make([]*dataRecord, 0, cfg.CollectionMaxCount),
		collChan:        make(chan *dataRecord),
		flushWg:         sync.WaitGroup{},
		flushJobs:       make(chan *flushJob),
		flushResults:    make(chan *flushResult, cfg.Concurrency), // buffered channel
		flushTicker:     cfg.clock.Ticker(cfg.FlushInterval),
		aggTicker:       cfg.clock.Ticker(cfg.FlushInterval),
		shardResults:    make(chan []*shard),
		newIdleWaiter:   make(chan chan struct{}),
		idleWaiters:     make([]chan struct{}, 0),
		inFlightRecords: 0,
		state:           &state,
		stopped:         make(chan struct{}),
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
	// every public API checks the state pre-conditions
	switch p.setState(producerStateStarting) {
	case producerStateUnstarted:
		// all good
	case producerStateStarted:
		return ErrProducerStarted
	case producerStateStopping, producerStateStopped:
		return ErrProducerStopped
	}

	// load all shards
	shards, err := p.listShards(ctx)
	if err != nil {
		p.setState(producerStateStopped)
		close(p.stopped)
		return err
	}

	p.cfg.Log.Info(fmt.Sprintf("Found %d shards", len(shards)))
	p.shards = shards

	// start the workers to flush the records to Kinesis
	for i := 0; i < p.cfg.Concurrency; i++ {
		p.flushWg.Add(1)
		go p.flushWorker(ctx, fmt.Sprintf("%02d", i))
	}

	// start the shard watcher
	go p.watchShards(ctx)

	// mark the producer as started
	p.setState(producerStateStarted)

	// always clean up after ourselves.
	defer p.shutdown()

	// start the event loop
	for {

		if p.inFlightRecords == 0 {
			p.notifyIdleWaiters()
		}

		select {
		case <-ctx.Done():
			return nil
		case <-p.aggTicker.C:
			p.collectAggregators(ctx)
		case idleWaiter := <-p.newIdleWaiter:
			p.idleWaiters = append(p.idleWaiters, idleWaiter)
		case newShards := <-p.shardResults:
			p.handleNewShards(ctx, newShards)
		case <-p.flushTicker.C:
			p.flush(ctx, "interval")
		case result := <-p.flushResults:
			p.handleFlushResult(ctx, result)
		case record := <-p.collChan:
			p.inFlightRecords += 1 // increment outside of collectRecord (aggregated records would be counted twice)
			p.collectRecord(ctx, record)
		case record := <-p.aggChan:
			p.aggRecord(ctx, record)
		}
	}
}

// Put puts the given data using the given partition key. This method is
// thread-safe and will exhibit backpressure by blocking the Put in case the
// Producer cannot keep up with transmitting the events to Kinesis. The
// [Producer] does some local rate limit accounting and throttle the
// transmission accordingly. Similarly, when Kinesis reports that we exceeded
// some limit, the Producer will also throttle and retry any failed
// transmissions.
//
// The context can be used to put a deadline on the Put so that if the data
// cannot be queued within some defined deadline, you can opt out of
// transmitting this event.
//
// If an unrecoverable error occurs while transmitting the data, the [Notifiee]
// will be notified about a [Notifiee.DroppedRecord].
func (p *Producer) Put(ctx context.Context, partitionKey string, data []byte) error {
	rec, err := newDataRecord(partitionKey, nil, data)
	if err != nil {
		return fmt.Errorf("put record: %w", err)
	}

	return p.PutRecord(ctx, rec)
}

// PutRecord enqueues the given record for tranmission to Kinesis. This method
// is thread-safe and will exhibit backpressure by blocking the PutRecord call
// in case the Producer cannot keep up with transmitting the events to Kinesis.
// The [Producer] does some local rate limit accounting and throttle the
// transmission accordingly. Similarly, when Kinesis reports that we exceeded
// some limit, the Producer will also throttle and retry any failed
// transmissions.
//
// The context can be used to put a deadline on the enqueue operation so that if
// the data cannot be queued within some defined deadline, you can opt out of
// transmitting this event.
//
// If an unrecoverable error occurs while transmitting the data, the [Notifiee]
// will be notified about a [Notifiee.DroppedRecord]. In there you can type cast
// the [Record] to the original record you passed in here. The [Producer] keeps
// a reference to the original record.
func (p *Producer) PutRecord(ctx context.Context, record Record) error {
	// verify that the producer is started
	state := p.state.Load()
	if state == int32(producerStateStarting) {
		if err := p.WaitIdle(ctx); err != nil {
			return err
		}
	} else if state != int32(producerStateStarted) {
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
		drecord, err = newDataRecord(record.PartitionKey(), record.ExplicitHashKey(), record.Data(), record)
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

	// try to enqueue the record
	select {
	case <-ctx.Done():
		return ctx.Err()
	case recChan <- drecord:
		return nil
	}
}

// WaitIdle blocks until the Producer has processed and transmitted all records.
// After this call returns, there are no in-flight requests or pending records
// waiting to be transmitted.
func (p *Producer) WaitIdle(ctx context.Context) error {
	waitChan := make(chan struct{})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopped:
		return ErrProducerStopped
	case p.newIdleWaiter <- waitChan:
		// pass
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopped:
		return ErrProducerStopped
	case <-waitChan:
		return nil
	}
}

// WaitStopped blocks until the Producer has stopped and cleaned up all its
// resources.
func (p *Producer) WaitStopped(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopped:
		return nil
	}
}

func (p *Producer) notifyIdleWaiters() {
	for _, idleWaiter := range p.idleWaiters {
		close(idleWaiter)
	}
	p.idleWaiters = make([]chan struct{}, 0)
}

// handleRetry handles a failed transmission of the given record. It first
// checks if the retry limit was exceeded and if so, notifies the [Notifiee]
// about a dropped record. Otherwise, the record will just be enqueued again.
// TODO: it would be even better if we enqueued it at the front.
func (p *Producer) handleRetry(ctx context.Context, record *dataRecord) {
	if record.retries >= p.cfg.RetryLimit {
		p.notifyDropped(ctx, record)
	} else {
		p.collectRecord(ctx, record)
	}
}

// notifyDropped notifies the user about dropped records. This could happen
// because the maximum number of transmission retries is succeeded or they are
// still buffered when the producer stopped. It returns the plain [dataRecord]
// if no user records are associated with it. This can happen if the user calls
// Put and passes the partition key and data manually. Otherwise, it loops
// through all user records. There could be many as an aggregated record could
// hold multiple user records.
func (p *Producer) notifyDropped(ctx context.Context, rec *dataRecord) {
	// if there's no user record associated with this dataRecord,
	// just plainly submit it.
	if len(rec.urecs) == 0 {
		p.inFlightRecords -= 1
		p.cfg.Notifiee.DroppedRecord(ctx, rec)
		return
	}

	// we have at least one user record, notify the user about the original Record
	for _, urec := range rec.urecs {
		// if the user record is also a [dataRecord], we unwrap it again by
		// making a recursive call.
		if dr, ok := urec.(*dataRecord); ok {
			p.notifyDropped(ctx, dr)
		} else {
			p.inFlightRecords -= 1
			p.cfg.Notifiee.DroppedRecord(ctx, urec)
		}
	}
}

// setState sets the producer state and logs the state transition
func (p *Producer) setState(new producerState) producerState {
	old := p.state.Swap(int32(new))
	p.cfg.Log.Info(fmt.Sprintf("Kinesis producer %s", new.String()), "old", producerState(old).String(), "new", new.String())
	return producerState(old)
}

func (p *Producer) shutdown() {
	defer close(p.stopped)
	p.setState(producerStateStopping)
	defer p.setState(producerStateStopped)

	// use own shutdown context
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range p.shardResults {
		// drain shardResults, exits the loop when shardResults was closed
	}

	// signal that there will be no new flush jobs
	close(p.flushJobs)

	// wait until the flush workers have returned
	p.flushWg.Wait()

	// close the job results channel as the workers have stopped
	close(p.flushResults)

	// since the flushResults channel is buffered ther might still be unhandled
	// events.
	for result := range p.flushResults {
		if result.err != nil {
			// if the entire request failed, notify the user about all records
			for _, rec := range result.job.records {
				p.notifyDropped(shutdownCtx, rec)
			}
		} else if result.out != nil && result.out.FailedRecordCount != nil && *result.out.FailedRecordCount > 0 {
			// if we experienced a partial failure, only notify the user about
			// these records.
			for i, rec := range result.out.Records {
				if rec.ShardId != nil || rec.SequenceNumber != nil || rec.ErrorMessage == nil || rec.ErrorCode == nil {
					continue
				}
				p.notifyDropped(shutdownCtx, result.job.records[i])
			}
		}
	}

	for _, rec := range p.collBuf {
		p.notifyDropped(shutdownCtx, rec)
	}

	p.drainChan(shutdownCtx, p.aggChan)
	p.drainChan(shutdownCtx, p.collChan)

	close(p.aggChan)
	close(p.collChan)
}

// drainChan iterates over the given channel until its closed or there are no
// elements to consume anymore.
func (p *Producer) drainChan(ctx context.Context, c chan *dataRecord) {
	for {
		select {
		case rec, more := <-c:
			if more {
				p.notifyDropped(ctx, rec)
			} else {
				return
			}
		default:
			return
		}
	}
}

// watchShards starts an infinite loop to periodically list all the shards of
// the given data stream. The probe frequency can be configured with
// [ProducerConfig.ShardUpdateInterval]. The returned shards are transmitted
// back to the main loop and handled there in [handleNewShards].
func (p *Producer) watchShards(ctx context.Context) {
	p.cfg.Log.Debug("Started watching shards")
	defer p.cfg.Log.Debug("Stopped watching shards")

	defer close(p.shardResults)

	t := p.cfg.clock.Ticker(p.cfg.ShardUpdateInterval)
	for {
		select {
		case <-ctx.Done():
			t.Stop()
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

// listShards requests the currently active lists of shards from AWS Kinesis.
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
	size         int
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
		size:         0,
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
		logger.Info("Flush records", "count", len(input.Records), "size", fmtSize(job.collSize))
		out, err := p.client.PutRecords(ctx, input)
		logger.Debug("Flushed records", "count", len(input.Records), "size", fmtSize(job.collSize))

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

	flushAt := p.cfg.clock.Now()
	for i := range p.shards {
		s := p.shards[i]

		res := s.sizeLimiter.ReserveN(p.cfg.clock.Now(), s.size)
		if p.cfg.clock.Now().Add(res.Delay()).After(flushAt) {
			flushAt = p.cfg.clock.Now().Add(res.Delay())
		}

		res = s.countLimiter.ReserveN(p.cfg.clock.Now(), s.count)
		if p.cfg.clock.Now().Add(res.Delay()).After(flushAt) {
			flushAt = p.cfg.clock.Now().Add(res.Delay())
		}

		s.size = 0
		s.count = 0
	}

	job := &flushJob{
		collSize: p.collSize,
		records:  p.collBuf,
		flushAt:  flushAt,
	}

	p.cfg.Log.Debug("Schedule flush", "count", len(p.collBuf), "size", fmtSize(p.collSize), "reason", reason)
	select {
	case <-ctx.Done():
		for _, rec := range job.records {
			p.notifyDropped(ctx, rec)
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

	if p.shards[shardIdx].size+recordSize > maxShardBytesPerS {
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
	p.shards[shardIdx].size += recordSize
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

func (p *Producer) collectAggregators(ctx context.Context) {
	nextDrain := p.cfg.FlushInterval

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
			nextDrain = p.cfg.clock.Until(s.aggregator.lastDrain)
		}
	}

	p.aggTicker.Reset(nextDrain)
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
	p.inFlightRecords += 1
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
		for _, rec := range result.job.records {
			if len(rec.urecs) == 0 {
				p.inFlightRecords -= 1
			} else {
				p.inFlightRecords -= len(rec.urecs) // TODO: go another level deeper?
			}
		}
		return
	}

	p.cfg.Log.Warn("Flush partial failure", "workerID", result.id, "failed", *result.out.FailedRecordCount, "total", len(result.out.Records))
	p.meterFlushCount.Add(ctx, int64(*result.out.FailedRecordCount), metric.WithAttributes(attribute.String("type", "failed")))

	// we have failed records, search for them in the result set and put
	// them in the retry queue.

	for i, rec := range result.out.Records {
		if rec.ShardId != nil || rec.SequenceNumber != nil || rec.ErrorMessage == nil || rec.ErrorCode == nil {
			if len(result.job.records[i].urecs) == 0 {
				p.inFlightRecords -= 1
			} else {
				p.inFlightRecords -= len(result.job.records[i].urecs) // TODO: go another level deeper?
			}
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

	p.cfg.Log.Info("Resharding", "new_shards", len(newShards))

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
