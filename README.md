# Docstore Go Lock

A simple binary lock (mutex) library built on top of  Docstore (Firestore, DynamoDB or MongoDB): https://gocloud.dev/howto/docstore/

Locks can be used for leader election or more custom use cases.
Locks have a TTL and each resource has its own document in the collection.
Locks acquire a specific Resource ID.