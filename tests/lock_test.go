package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bgadrian/doclock"
	"github.com/golang/mock/gomock"
	"gocloud.dev/docstore"
)

const cid = "res_id_1"

func asDoc(doc docstore.Document) *doclock.Doc {
	return doc.(*doclock.Doc)
}

func TestLock_Simple(t *testing.T) {
	c := gomock.NewController(t)
	coll := NewMockCollection(c)
	ctx := context.Background()
	lock := doclock.New(coll, cid).Build()

	coll.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, doc docstore.Document, fps ...docstore.FieldPath) error {
			d := asDoc(doc)
			if d.ID != cid {
				t.Errorf("Doc ID exp: %q received: %q", cid, d.ID)
			}
			return nil
		})
	coll.EXPECT().Put(gomock.Any(), gomock.Any())

	lostCh, err := lock.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lostCh == nil {
		t.Errorf("lost channel was nil")
	}

	coll.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, doc docstore.Document) error {
		d := asDoc(doc)
		if d.ID != cid {
			t.Errorf("Doc ID exp: %q received: %q", cid, d.ID)
		}
		return nil
	}).Times(1) //mandatory only once
	err = lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	//expect consecutive Unlocks to fail
	err = lock.Unlock()
	if err != doclock.ErrNotHeld {
		t.Fatal("should have failed because lock was lost already")
	}
}

func TestLock_Contention(t *testing.T) {
	c := gomock.NewController(t)
	coll := NewMockCollection(c)
	ctx := context.Background()
	lock := doclock.New(coll, cid).Build()

	coll.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, doc docstore.Document, fps ...docstore.FieldPath) error {
			d := asDoc(doc)
			if d.ID != cid {
				t.Errorf("Doc ID exp: %q received: %q", cid, d.ID)
			}
			return nil
		}).AnyTimes()
	coll.EXPECT().Put(gomock.Any(), gomock.Any()).AnyTimes()

	lostCh, err := lock.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lostCh == nil {
		t.Errorf("lost channel was nil")
	}

	//other jobs should block
	count := 3
	ctxTimeouts, cancel := context.WithTimeout(context.Background(), time.Second*3)
	//if a deadlock arises, we do our best and close the test
	go func() {
		time.Sleep(time.Second * 10)
		cancel()
	}()

	wg := sync.WaitGroup{}
	var successfullyAquired int64
	wg.Add(count)
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer wg.Done()

			otherColl := NewMockCollection(c)
			otherColl.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, doc docstore.Document, fps ...docstore.FieldPath) error {
					d := asDoc(doc)
					d.ExpirationTime = time.Now().Add(time.Second) //always in the future
					d.ExpirationActor = 0
					return nil
				}).AnyTimes()
			otherLock := doclock.New(otherColl, fmt.Sprintf("other_%d", i)).Build()

			//this should end with Context error timeout
			ch, err := otherLock.Lock(ctxTimeouts)
			if ch != nil || err == nil {
				fmt.Println("other worker got the lock instead")
				atomic.AddInt64(&successfullyAquired, 1)
			}
		}(i)
	}
	wg.Wait()
	if successfullyAquired > 0 {
		t.Fatal("other goroutines got the lock too")
	}

	coll.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, doc docstore.Document) error {
		d := asDoc(doc)
		if d.ID != cid {
			t.Errorf("Doc ID exp: %q received: %q", cid, d.ID)
		}
		return nil
	})
	err = lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}
