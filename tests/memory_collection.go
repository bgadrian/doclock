package tests

import (
	"context"
	"errors"
	"sync"

	"github.com/bgadrian/doclock"
	"gocloud.dev/docstore"
)

type InMemoryCollection struct {
	m       sync.Mutex
	storage map[string]doclock.Doc
}

func NewInMemoryCollection() *InMemoryCollection {
	return &InMemoryCollection{
		storage: map[string]doclock.Doc{},
	}
}

func (m *InMemoryCollection) Put(ctx context.Context, doc docstore.Document) error {
	m.m.Lock()
	defer m.m.Unlock()
	asDoc := doc.(*doclock.Doc)
	m.storage[asDoc.ID] = *asDoc
	return nil
}

func (m *InMemoryCollection) Delete(ctx context.Context, doc docstore.Document) error {
	m.m.Lock()
	defer m.m.Unlock()
	asDoc := doc.(*doclock.Doc)
	delete(m.storage, asDoc.ID)
	return nil
}

func (m *InMemoryCollection) Get(ctx context.Context, doc docstore.Document, fps ...docstore.FieldPath) error {
	m.m.Lock()
	defer m.m.Unlock()
	asDoc := doc.(*doclock.Doc)
	stored, ok := m.storage[asDoc.ID]
	if !ok {
		return errors.New("not found")
	}
	asDoc.ExpirationTime = stored.ExpirationTime
	asDoc.ExpirationActor = stored.ExpirationActor
	asDoc.DocstoreRevision = stored.DocstoreRevision
	return nil
}
