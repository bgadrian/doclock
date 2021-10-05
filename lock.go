package docstore_leader_go

import (
	"context"
	"errors"
	"sync"
	"time"

	"gocloud.dev/docstore"
	"gocloud.dev/gcerrors"
	"golang.org/x/exp/rand"
)

// KEY is the collection name of the Key property.
const KEY = "id"

var (
	ErrLockHeld = errors.New("lock already held")
	ErrNotHeld  = errors.New("lock not held")
)

// Doc is the internal document. Exported only for marshalling.
type Doc struct {
	ID              string `docstore:"id"` //the resourceID
	ExpirationTime  time.Time
	ExpirationActor uint64

	//for optimistic concurrency
	DocstoreRevision interface{}
}

func (d *Doc) isExpired() bool {
	return d.ExpirationTime.Before(time.Now())
}
func (d *Doc) hasLease(actorID uint64) bool {
	return !d.isExpired() && d.ExpirationActor == actorID
}

type Lock struct {
	collection *docstore.Collection
	resourceID string
	actorID    uint64
	acquired   bool
	m          sync.Mutex

	lockPollDuration       time.Duration
	leaseTTLUpdateInterval time.Duration
	leaseTTL               time.Duration
}

type LockBuilder struct {
	l *Lock
}

func New(collection *docstore.Collection, resourceID string) *LockBuilder {
	return &LockBuilder{l: &Lock{
		collection: collection,
		resourceID: resourceID,
		actorID:    nonZeroRandom(),

		//TODO expose these as builder methods
		lockPollDuration:       time.Millisecond * 500,
		leaseTTLUpdateInterval: time.Second * 9,
		leaseTTL:               time.Second * 30,
	},
	}
}

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
		if err != nil {
			return nil, err
		}

		//this actor already has an active lease on it, this is a resume
		if doc.hasLease(l.actorID) {
			lostCh := l.afterAcquisition()
			return lostCh, nil
		}
		if doc.isExpired() {
			doc.ExpirationActor = l.actorID
			doc.ExpirationTime = time.Now().Add(l.leaseTTL)

			err = l.collection.Put(ctx, &doc)
			if err != nil {
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
		}
	}
}

func (l *Lock) afterAcquisition() <-chan struct{} {
	l.acquired = true

	c := make(chan struct{})
	go l.updateLease(l.actorID, c)
	return c
}

// updateLease keeps the lease updated as long as it can
// it should not access l properties as it is not thread safe
func (l *Lock) updateLease(actorID uint64, lostSignal chan struct{}) {
	defer close(lostSignal)
	ticker := time.NewTicker(l.leaseTTLUpdateInterval)
	defer ticker.Stop()

	doc := Doc{ID: l.resourceID}
	for {
		//as best effort to keep the lease, we first update it, then wait the tick

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
		//OK
		<-ticker.C
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
	return l.collection.Delete(context.Background(), &doc)
}

func nonZeroRandom() uint64 {
	for {
		r := rand.Uint64()
		if r != 0 {
			return r
		}
	}
}
