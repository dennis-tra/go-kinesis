package kinesis

import (
	"crypto/md5"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/dennis-tra/go-kinesis/pb"
)

// pbTagSize is the number of bytes used for encoding
// the wire type and field number in protobuf. Because
// all field numbers are < 16, this number will always
// be one.
const pbTagSize = 1

var (
	magicBytes          = []byte{0xF3, 0x89, 0x9A, 0xC2}
	ErrNoRecordsToDrain = fmt.Errorf("no records to drain")
	baseAggRecordSize   = len(magicBytes) + md5.Size
)

// aggregator
// Layout of an aggregated message:
//
//	0               4                  N          N+15
//	+---+---+---+---+==================+---+...+---+
//	|  MAGIC NUMBER | PROTOBUF MESSAGE |    MD5    |
//	+---+---+---+---+==================+---+...+---+
type aggregator struct {
	aggSize   int
	buf       []*dataRecord
	keys      []string
	keysIdx   map[string]int
	lastDrain time.Time
	clk       clock.Clock
}

func newAggregator(clk clock.Clock) *aggregator {
	return &aggregator{
		aggSize:   baseAggRecordSize,
		buf:       make([]*dataRecord, 0),
		keys:      make([]string, 0),
		keysIdx:   make(map[string]int),
		lastDrain: clk.Now(),
		clk:       clk,
	}
}

func (a *aggregator) drain() (*dataRecord, error) {
	if len(a.buf) == 0 {
		return nil, ErrNoRecordsToDrain
	}

	kclData, err := a.aggregate()
	if err != nil {
		return nil, err
	}

	rec, err := newDataRecord(a.keys[0], nil, kclData)
	if err != nil {
		return nil, fmt.Errorf("new data record: %w", err)
	}

	a.aggSize = baseAggRecordSize
	a.buf = make([]*dataRecord, 0)
	a.keys = make([]string, 0)
	a.keysIdx = make(map[string]int)
	a.lastDrain = a.clk.Now()

	return rec, nil
}

func (a *aggregator) aggregate() ([]byte, error) {
	records := make([]*pb.Record, len(a.buf))
	for i, rec := range a.buf {
		keyIndex := uint64(a.keysIdx[rec.pk])
		records[i] = &pb.Record{
			Data:              rec.data,
			PartitionKeyIndex: &keyIndex,
		}
	}

	rec := &pb.AggregatedRecord{
		PartitionKeyTable: a.keys,
		Records:           records,
	}

	pbData, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	hasher := md5.New()
	hasher.Write(pbData)
	checksum := hasher.Sum(nil)

	kclData := append(magicBytes, pbData...)
	kclData = append(kclData, checksum...)

	return kclData, nil
}

func (a *aggregator) put(record *dataRecord) {
	a.aggSize = a.sizeWith(record)

	if _, exists := a.keysIdx[record.pk]; !exists {
		a.keysIdx[record.pk] = len(a.keys)
		a.keys = append(a.keys, record.pk)
	}

	a.buf = append(a.buf, record)
}

// sizeWith returns the total size of the aggregated record if the given record
// was included in the aggregation.
func (a *aggregator) sizeWith(record *dataRecord) int {
	aggSize := a.aggSize

	idx, exists := a.keysIdx[record.pk]
	if !exists {
		idx = len(a.keys)
		aggSize += protowire.SizeBytes(len(record.pk)) + pbTagSize
	}

	recSize := protowire.SizeVarint(uint64(idx)) + pbTagSize
	recSize += protowire.SizeBytes(len(record.data)) + pbTagSize

	aggSize += protowire.SizeBytes(recSize) + pbTagSize

	return aggSize
}

func (a *aggregator) count() int {
	return len(a.buf)
}
