/* Package doclock  is a simple binary lock (mutex) library built on top of  Docstore (Firestore, DynamoDB or MongoDB): https:gocloud.dev/howto/docstore/
Each resource ID will have its own document in a collection.
Active lock will be stored in each document, with a TTL.
*/

package doclock

// go install github.com/golang/mock/mockgen@v1.6.0
//go:generate mockgen -source=lock.go --destination=tests/mock_datasource.go --package tests

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"

	"gocloud.dev/docstore"
	"gocloud.dev/gcerrors"
)

// KEY is the collection name of the Key property.
const KEY = "id"

var (
	ErrLockHeld = errors.New("lock already held")
	ErrNotHeld  = errors.New("lock not held")
)

// Collection is a subset of methods of *docstore.Collection
//TODO make this more abstract, and implement a wrapper to use any other storage
type Collection interface {
	Put(ctx context.Context, doc docstore.Document) error
	Delete(ctx context.Context, doc docstore.Document) error
	Get(ctx context.Context, doc docstore.Document, fps ...docstore.FieldPath) error
}

type Lock struct {
	collection Collection
	resourceID string
	actorID    uint64
	acquired   bool
	m          sync.Mutex
	//closeKeeper chan struct{}

	lockPollDuration       time.Duration
	leaseTTLUpdateInterval time.Duration
	leaseTTL               time.Duration
}

type LockBuilder struct {
	l *Lock
}

func New(collection Collection, resourceID string) *LockBuilder {
	return &LockBuilder{
		l: &Lock{
			collection: collection,
			resourceID: resourceID,
			actorID:    nonZeroRandom(),

			lockPollDuration:       time.Millisecond * 500,
			leaseTTLUpdateInterval: time.Second * 9,
			leaseTTL:               time.Second * 30,
		},
	}
}

// WithLockPollDuration sets a custom maximum duration between 2 attempts to acquire the lock
func (b *LockBuilder) WithLockPollDuration(d time.Duration) *LockBuilder {
	b.l.lockPollDuration = d
	return b
}

// WithLeaseTTLUpdateInterval sets a custom update rate of the extension of lease/TTL
// A document.Update with now+LeaseTTL will be called at each tick, while the lock is acquired.
func (b *LockBuilder) WithLeaseTTLUpdateInterval(d time.Duration) *LockBuilder {
	b.l.leaseTTLUpdateInterval = d
	return b
}

// WithLeaseTTL sets a custom TTL for the lock
func (b *LockBuilder) WithLeaseTTL(d time.Duration) *LockBuilder {
	b.l.leaseTTL = d
	return b
}

// WithActorID sets a custom ActorID. It is not recommended!
func (b *LockBuilder) WithActorID(d uint64) *LockBuilder {
	b.l.actorID = d
	return b
}

// Build builds a lock instance
func (b *LockBuilder) Build() *Lock {
	return b.l
}

// Lock will begin to poll to acquire a lock. Blocks until acquired. Cancel the ctx to stop it.
// Returns a channel which will be closed when the lock is lost OR failure occurs.
func (l *Lock) Lock(ctx context.Context) (<-chan struct{}, error) {
	l.m.Lock()
	defer l.m.Unlock()

	if l.acquired {
		return nil, ErrLockHeld
	}

	doc := Doc{ID: l.resourceID}

	for {
		err := l.collection.Get(ctx, &doc)
		var notFound bool
		if err != nil {
			notFound = isNotFound(err)
			if !notFound {
				return nil, err
			}
		}

		//this actor already has an active lease on it, this is a resume
		if doc.hasLease(l.actorID) {
			lostCh := l.afterAcquisition()
			return lostCh, nil
		}
		if notFound || doc.isExpired() {
			doc.ExpirationActor = l.actorID
			doc.ExpirationTime = time.Now().Add(l.leaseTTL)

			err = l.collection.Put(ctx, &doc)
			if err != nil {
				//TODO retry/continue when the error is from optimistic locking
				//it means 2 concurrent locks created the doc
				return nil, err
			}
			//successfully acquired the lock
			lostCh := l.afterAcquisition()
			return lostCh, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err() //stop trying, the actor doesn't want it anymore
		case <-time.After(doc.ExpirationTime.Sub(time.Now()) + time.Millisecond*5):
		//retry after we know for sure we have a chance
		case <-time.After(l.lockPollDuration):
			//retry as a poll, maybe the leader unlocked or died
		}
	}
}

func (l *Lock) afterAcquisition() <-chan struct{} {
	l.acquired = true

	c := make(chan struct{})
	//l.closeKeeper = make(chan struct{})
	go l.updateLease(l.actorID, c)
	return c
}

// updateLease keeps the lease updated as long as it can
// it should not access l properties as it is not thread safe
func (l *Lock) updateLease(actorID uint64, lostSignal chan struct{}) {
	ticker := time.NewTicker(l.leaseTTLUpdateInterval)

	defer func() {
		close(lostSignal)
		ticker.Stop()
	}()

	doc := Doc{ID: l.resourceID}
	for {
		<-ticker.C

		err := l.collection.Get(context.Background(), &doc)
		if err != nil {
			code := gcerrors.Code(err)
			if code == gcerrors.NotFound {
				//if the document is not found it is ok, it means Unlock was called
				return
			}
			//TODO add logger
			//TODO add retrier
			break
		}

		if !doc.hasLease(actorID) {
			//we have lost the lease
			return
		}
		doc.ExpirationTime = time.Now().Add(l.leaseTTL)

		//the optimistic concurrency ensures that if we lost the lease
		//between the Get and Put, our update will fail, and at the
		//next tick we'll close this goroutine
		err = l.collection.Put(context.Background(), &doc)
		if err != nil {
			//TODO add logger
			//TODO add retrier
			break
		}
	}
}

// Unlock removes the resource lock. The operation is sync/blocking.
// The channel returned by Lock() will be closed soon after, async.
func (l *Lock) Unlock() error {
	l.m.Lock()
	defer l.m.Unlock()

	if !l.acquired {
		return ErrNotHeld
	}
	doc := Doc{ID: l.resourceID}

	//TODO add retrier
	//TODO trigger a close for updateLease
	err := l.collection.Delete(context.Background(), &doc)
	l.actorID = 0
	l.acquired = false
	//close(l.closeKeeper)
	return err
}

func nonZeroRandom() uint64 {
	for {
		r := rand.Uint64()
		if r != 0 {
			return r
		}
	}
}

func isNotFound(err error) bool {
	if strings.Contains(err.Error(), "not found") {
		return true
	}
	return gcerrors.Code(err) == gcerrors.NotFound
}
