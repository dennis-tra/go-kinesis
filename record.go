package kinesis

import (
	"crypto/md5"
	"fmt"
	"math/big"
)

type Record interface {
	// PartitionKey determines which shard in the stream the data record is
	// assigned to. Partition keys are Unicode strings with a maximum length
	// limit of 256 characters for each key. Amazon Kinesis Data Streams uses the
	// partition key as input to a hash function that maps the partition key and
	// associated data to a specific shard. Specifically, an MD5 hash function is
	// used to map partition keys to 128-bit integer values and to map associated
	// data records to shards. As a result of this hashing mechanism, all data
	// records with the same partition key map to the same shard within the stream.
	PartitionKey() string

	// ExplicitHashKey returns an optional explicit hash key that will be used
	// for shard mapping. Should return nil if there is none.
	ExplicitHashKey() *string

	// Data is the blob to put into the record, which is base64-encoded when the
	// blob is serialized. When the data blob (the payload before
	// base64-encoding) is added to the partition key size, the total size must
	// not exceed the maximum record size (1 MB).
	Data() []byte
}

type dataRecord struct {
	pk      string   // partition key
	ehk     *string  // explicit hash key
	hk      *big.Int // hash key
	data    []byte   // payload data
	retries int      // number of retries
}

func newDataRecord(partitionKey string, explicitHashKey *string, data []byte) (*dataRecord, error) {
	var hk *big.Int
	if explicitHashKey != nil {
		val, ok := new(big.Int).SetString(*explicitHashKey, 10)
		if !ok {
			return nil, fmt.Errorf("cast explicit hash key %s to big int", *explicitHashKey)
		}
		hk = val
	} else {
		h := md5.New()
		h.Write([]byte(partitionKey))
		hash := h.Sum(nil)
		hk = new(big.Int).SetBytes(hash[:])
	}

	rec := &dataRecord{
		pk:      partitionKey,
		ehk:     explicitHashKey,
		hk:      hk,
		data:    data,
		retries: 0,
	}
	return rec, nil
}

var _ Record = (*dataRecord)(nil)

func (d *dataRecord) PartitionKey() string {
	return d.pk
}

func (d *dataRecord) ExplicitHashKey() *string {
	return d.ehk
}

func (d *dataRecord) Data() []byte {
	return d.data
}

func (d *dataRecord) Size() int {
	return len(d.data) + len(d.pk)
}
