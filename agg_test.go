package kinesis

import (
	"testing"

	"github.com/benbjohnson/clock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregator_new(t *testing.T) {
	agg := newAggregator(clock.New())

	assert.Equal(t, baseAggRecordSize, agg.aggSize)
	assert.NotNil(t, agg.buf)
	assert.NotNil(t, agg.keys)
	assert.NotNil(t, agg.keysIdx)
	assert.False(t, agg.lastDrain.IsZero())
}

func TestAggregator_put(t *testing.T) {
	agg := newAggregator(clock.New())

	rec1 := &dataRecord{
		pk:   "partition_key",
		data: []byte("payload_data"),
	}
	agg.put(rec1)

	assert.Len(t, agg.buf, 1)
	assert.Len(t, agg.keys, 1)
	assert.Len(t, agg.keysIdx, 1)
	assert.Equal(t, rec1, agg.buf[0])
	assert.Equal(t, rec1.pk, agg.keys[0])
	assert.Equal(t, 0, agg.keysIdx[rec1.pk])

	kcl, err := agg.aggregate()
	require.NoError(t, err)
	assert.Equal(t, len(kcl), agg.aggSize)

	rec2 := &dataRecord{
		pk:   "partition_key",
		data: []byte("other_data"),
	}
	agg.put(rec2)

	assert.Len(t, agg.buf, 2)
	assert.Len(t, agg.keys, 1)
	assert.Len(t, agg.keysIdx, 1)
	assert.Equal(t, rec1, agg.buf[0])
	assert.Equal(t, rec2, agg.buf[1])
	assert.Equal(t, rec1.pk, agg.keys[0])
	assert.Equal(t, rec2.pk, agg.keys[0])
	assert.Equal(t, 0, agg.keysIdx[rec1.pk])
	assert.Equal(t, 0, agg.keysIdx[rec2.pk])

	kcl, err = agg.aggregate()
	require.NoError(t, err)
	assert.Equal(t, len(kcl), agg.aggSize)

	rec3 := &dataRecord{
		pk:   "other_key",
		data: []byte("other_data"),
	}
	agg.put(rec3)

	assert.Len(t, agg.buf, 3)
	assert.Len(t, agg.keys, 2)
	assert.Len(t, agg.keysIdx, 2)
	assert.Equal(t, rec1, agg.buf[0])
	assert.Equal(t, rec2, agg.buf[1])
	assert.Equal(t, rec3, agg.buf[2])
	assert.Equal(t, rec1.pk, agg.keys[0])
	assert.Equal(t, rec2.pk, agg.keys[0])
	assert.Equal(t, rec3.pk, agg.keys[1])
	assert.Equal(t, 0, agg.keysIdx[rec1.pk])
	assert.Equal(t, 0, agg.keysIdx[rec2.pk])
	assert.Equal(t, 1, agg.keysIdx[rec3.pk])

	kcl, err = agg.aggregate()
	require.NoError(t, err)
	assert.Equal(t, len(kcl), agg.aggSize)
}

func TestAggregator_drain(t *testing.T) {
	t.Run("no_records", func(t *testing.T) {
		agg := newAggregator(clock.New())
		rec, err := agg.drain()
		assert.Error(t, err)
		assert.Nil(t, rec)
	})

	t.Run("reset_fields", func(t *testing.T) {
		agg := newAggregator(clock.New())

		rec := &dataRecord{
			pk:   "partition_key",
			data: []byte("payload_data"),
		}
		agg.put(rec)

		lastAgg := agg.lastDrain

		aggRec, err := agg.drain()
		assert.NoError(t, err)
		assert.NotNil(t, aggRec)

		assert.True(t, lastAgg.Before(agg.lastDrain))
		assert.Equal(t, baseAggRecordSize, agg.aggSize)
		assert.Len(t, agg.buf, 0)
		assert.Len(t, agg.keys, 0)
		assert.Len(t, agg.keysIdx, 0)
	})
}

func TestAggregator_sizeWith(t *testing.T) {
	agg := newAggregator(clock.New())

	rec := &dataRecord{
		pk:   "partition_key",
		data: []byte("payload_data"),
	}
	expected := agg.sizeWith(rec)
	assert.Greater(t, expected, agg.aggSize)

	agg.put(rec)
	assert.Equal(t, expected, agg.aggSize)

	aggRec, err := agg.drain()
	require.NoError(t, err)

	assert.Len(t, aggRec.data, expected)
}
