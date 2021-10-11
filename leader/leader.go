package leader

import (
	"context"

	"github.com/bgadrian/doclock"
)

type Leader interface {
	IsLeader()
	IsFollower()
}

// WrapLockWithLeader is a helper that calls the Leader methods based on Locks status
func WrapLockWithLeader(ctx context.Context, l *doclock.Lock, actor Leader) {
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
		<-leaderCh //blocks while this lock is acquired
		actor.IsFollower()
	}
}
