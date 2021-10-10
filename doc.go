package doclock

import "time"

// Doc is the internal document. Exported only for marshalling.
type Doc struct {
	ID              string    `docstore:"id"` //the resourceID
	ExpirationTime  time.Time `docstore:"ex_ts"`
	ExpirationActor uint64    `docstore:"ex_actor"`

	//for optimistic concurrency
	DocstoreRevision interface{}
}

func (d *Doc) isExpired() bool {
	return d.ExpirationTime.Before(time.Now())
}
func (d *Doc) hasLease(actorID uint64) bool {
	return !d.isExpired() && d.ExpirationActor == actorID
}
