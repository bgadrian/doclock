package doclock

import (
	"context"
)

//go:generate mockgen -source=leader.go --destination=tests/mock_leader.go --package tests
type Leader interface {
	IsLeader()
	IsFollower()
}

// WrapLockWithLeader is a helper that calls the Leader methods based on Locks status
// blocks as long as ctx is active and lock is acquired
func WrapLockWithLeader(ctx context.Context, l *Lock, actor Leader) {
	actor.IsFollower()
	for {
		if ctx.Err() != nil {
			return
		}
		leaderCh, err := l.Lock(ctx)
		if err != nil {
			//TODO log this
			continue //keep trying
		}
		actor.IsLeader()
		select {
		case <-ctx.Done():
			return
		case <-leaderCh: //blocks while this lock is acquired
			//falltrough
		}
		actor.IsFollower()
	}
}
