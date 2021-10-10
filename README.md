# DocLock - A locking Go library for cloud

A simple binary lock (mutex) library built on top of  Docstore (Firestore, DynamoDB or MongoDB): https://gocloud.dev/howto/docstore/

Locks can be used for leader election or more custom use cases.
Locks have a TTL and each resource has its own document in the collection.
Locks acquire a specific Resource ID.


# Usage

```go
//...
import "github.com/bgadrian/doclock"
import "gocloud.dev/docstore"
//...

func(){
	resourceID := "my_server_leader"
	//the collection needs to have the Key ID set `id`, you can use the constant KEY
	var myDataStoreCollection *docstore.Collection
	
	//blocks until acquired
	leaderLock := doclock.New(myDataStoreCollection, resourceID).Build()

    leaderCh, _  := leaderLock.Lock(context.Background())
	
	//do your leader stuff while leaderCh is open
	
	//when shutting down call to release the lock
	leaderLock.Unlock()
}
```
# Contribute
Requirements
```bash
Go 1.16+
go install github.com/golang/mock/mockgen
```