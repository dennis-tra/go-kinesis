package kinesis

import (
	"context"
	"log/slog"
)

type NotifieeBundle struct {
	DroppedRecordF func(context.Context, Record)
}

var _ Notifiee = (*NotifieeBundle)(nil)

func (n *NotifieeBundle) DroppedRecord(ctx context.Context, rec Record) {
	if n.DroppedRecordF != nil {
		n.DroppedRecordF(ctx, rec)
	}
}

type Notifiee interface {
	DroppedRecord(ctx context.Context, rec Record)
}

type NoopNotifiee struct{}

var _ Notifiee = (*NoopNotifiee)(nil)

func (n *NoopNotifiee) DroppedRecord(ctx context.Context, rec Record) {}

type LogNotifiee struct {
	Log *slog.Logger
}

var _ Notifiee = (*LogNotifiee)(nil)

func (l *LogNotifiee) DroppedRecord(ctx context.Context, rec Record) {
	l.Log.Warn("Dropped record", "partition", rec.PartitionKey())
}
