package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bgadrian/doclock"
	"github.com/golang/mock/gomock"
)

func TestLeader_Simple(t *testing.T) {
	c := gomock.NewController(t)
	ctx, finish := context.WithTimeout(context.Background(), time.Second*3)
	defer finish()

	lockOne := doclock.New(NewInMemoryCollection(), cid).
		WithLeaseTTLUpdateInterval(time.Millisecond * 33).
		WithLockPollDuration(time.Millisecond * 3).
		WithLeaseTTL(time.Millisecond * 100).
		Build()

	one := NewMockLeader(c)
	oneCtx, closeOne := context.WithCancel(ctx)
	defer closeOne()
	var oneLeader int32
	var oneLeaderAtLeastOnce int32
	one.EXPECT().IsFollower().Do(func() {
		atomic.SwapInt32(&oneLeader, 0)
	}).AnyTimes()
	one.EXPECT().IsLeader().Do(func() {
		atomic.SwapInt32(&oneLeader, 1)
		atomic.SwapInt32(&oneLeaderAtLeastOnce, 1)
	}).AnyTimes()

	lockTwo := doclock.New(NewInMemoryCollection(), cid).
		WithLeaseTTLUpdateInterval(time.Millisecond * 33).
		WithLockPollDuration(time.Millisecond * 3).
		WithLeaseTTL(time.Millisecond * 100).
		Build()
	two := NewMockLeader(c)
	twoCtx, closeTwo := context.WithCancel(ctx)
	defer closeTwo()
	var twoLeader int32
	var twoLeaderAtLeastOnce int32
	two.EXPECT().IsFollower().Do(func() {
		atomic.SwapInt32(&twoLeader, 0)
	}).AnyTimes()
	two.EXPECT().IsLeader().Do(func() {
		atomic.SwapInt32(&twoLeader, 1)
		atomic.SwapInt32(&twoLeaderAtLeastOnce, 1)
	}).AnyTimes()

	go doclock.WrapLockWithLeader(oneCtx, lockOne, one)
	go doclock.WrapLockWithLeader(twoCtx, lockTwo, two)

	for {
		//wait until one of them is leader
		if atomic.LoadInt32(&oneLeader) == 1 ||
			atomic.LoadInt32(&twoLeader) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
		if ctx.Err() != nil {
			t.Fatal("timeout, no leader yet")
		}
	}
	if atomic.LoadInt32(&oneLeader) == 1 &&
		atomic.LoadInt32(&twoLeader) == 1 {
		t.Fatal("we have now 2 leaders")
	}
	if atomic.LoadInt32(&oneLeaderAtLeastOnce) == 1 &&
		atomic.LoadInt32(&twoLeaderAtLeastOnce) == 1 {
		t.Fatal("we had both leaders")
	}
	if atomic.LoadInt32(&oneLeader) == 1 {
		closeOne()
	} else {
		closeTwo()
	}

	for {
		//both of them should have been at least once by now
		if atomic.LoadInt32(&oneLeaderAtLeastOnce) == 1 &&
			atomic.LoadInt32(&twoLeaderAtLeastOnce) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
		if ctx.Err() != nil {
			t.Fatal("timeout, no leader yet")
		}
	}
}
