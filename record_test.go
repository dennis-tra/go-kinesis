package kinesis

type testRecord struct {
	*dataRecord
	payload string
}
