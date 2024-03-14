package kinesis

import "log/slog"

type NotifieeBundle struct {
	DroppedRecordF func(Record)
}

var _ Notifiee = (*NotifieeBundle)(nil)

func (n *NotifieeBundle) DroppedRecord(rec Record) {
	if n.DroppedRecordF != nil {
		n.DroppedRecordF(rec)
	}
}

type Notifiee interface {
	DroppedRecord(rec Record)
}

type NoopNotifiee struct{}

var _ Notifiee = (*NoopNotifiee)(nil)

func (n *NoopNotifiee) DroppedRecord(rec Record) {}

type LogNotifiee struct {
	Log *slog.Logger
}

var _ Notifiee = (*LogNotifiee)(nil)

func (l *LogNotifiee) DroppedRecord(rec Record) {
	l.Log.Warn("Dropped record", "partition", rec.PartitionKey())
}
