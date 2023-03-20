# Location tracker


Consistency-aware durability + Causal consistency

Each object is an independent CRDT (Guarantees eventual convergence of replicas)

Async replication. It means that you can do writes on the primary, and it will replicate writes eventually. 

A trade-off between throughput and consistency.

The primary reflects some order of operations

Eventual durability:

1) Master-acknowledged writes. For each write, the associated entity writes to its local replica, and then using gossip (asynchronously) replicates it to other replicas.
2) Async replication (To ensure independent and simultaneous progress on various locations) makes writes eventually durable, 
but enables only weak consistency due to data loss upon failures. If a failure arises before writes are made durable (fully replicated), they can be lost.


Cross-client monotonic read: Every time the system satisfies a query, the value that it returns is at least as fresh as the one returned by the previous query to any client.

Key idea: Shift the point of durability from writes to reads. In other words, data is replicated and persisted before reads are served.


https://github.com/phiSgr/gatling-grpc/blob/master/src/test/scala/com/github/phisgr/example/GrpcExample.scala


```

http GET 127.0.0.1:8079/rides/cluster/members`

http GET 127.0.0.2:8079/rides/cluster/members`

```


```

http GET 127.0.0.1:8079/rides/cluster/shards/vehicles

http GET 127.0.0.2:8079/rides/cluster/shards/vehicles

```



 
`grpcurl -plaintext 127.0.0.1:8080 list`
`grpcurl -plaintext 127.0.0.2:8080 list`

```
com.rides.VehicleService
grpc.reflection.v1alpha.ServerReflection
```


`grpcurl -plaintext localhost:8080 list com.rides.VehicleService`


```
com.rides.VehicleService.GetCoordinates
com.rides.VehicleService.PostLocation
com.rides.VehicleService.Subscribe
```


`grpcurl -d '{"vehicleId":1,"lon":1,"lat":1.1}' -plaintext 127.0.0.1:8080 com.rides.VehicleService/PostLocation`


`grpcurl -d '{"vehicleId":5,"version":53}' -plaintext 127.0.0.2:8080 com.rides.VehicleService/GetCoordinates`

```
{
  "vehicleId": "5",
  "point": { "lon": 1, "lat": 1.2 },
  "statusCode": "NotDurable",
  "durabilityLevel": "Majority",
  "versions": { "current": "52", "requested": "53" }
}
```


`grpcurl -d '{"vehicleId":1}' -plaintext 127.0.0.1:8080 com.rides.VehicleService/Subscribe`



### TODOs

1. Borrowing an identity

a) Transient nodes borrow an `identity` (a dot) from permanent nodes.

b) Transient nodes use that `identity`(dot) when it updates `ReplicatedVehiclePB`.

#### Safe node retirement
A transient node that wants to leave should promise that it won't write again. Transient nodes should mark its dots as `inactive`.  


2. Replace VictorClocks with HybridTime ???
3. Bloom filter to reduce a number of actor recoveries for keys that don't exist (No need since we).
4. Range-based sharding or multiple `VehicleStatePB` per entity.
5. Dependency tracking.



Making CRDTs Byzantine FT
It would be desirable to have a more efficient protocol for nodes p and q to determine which updates they need to exchange so that, at the end, both have
delivered the same set of updates (an anti-entropy or reconciliation protocol). 

A common algorithm for this purpose is to use version vectors [34]: 
each node sequentially numbers the updates that it generates, and so the set of updates that a node has delivered can be summarised by remembering just
the highest sequence number from each node.




https://github.com/goncalotomas/FMKe/blob/master/doc/FMK_DataModel.pdf
https://medium.com/@ifesdjeen/database-papers-anti-entropy-without-merkle-trees-deletes-without-tombstones-a47d2b1608f3


************************************************************************************
MovR - allows personal vehicle to be shared among users.

a) Vehicles (manages the details about vehicles including their desc, location, and battery level).
b) Rides which manages the details of a ride including the start, end time, thr location(lat, lon) and a few other thing. (Events: `RideStarted`, `RideEnded`).
c) Users which manages user registration and login.

The problem we're trying to solve is that the Ride service determines if a vehicle is able to be rented or not. When a ride started or finished, the Rides informs the Vehicle by publishing `RideStarted`, `RideEnded` events. 

The Vehicle then can track internally if a vehicle is available.
The Vehicle get the event and update its internal state accordingly.

Ride request|order fulfilment

****************************************

Shared bank account - Safe replication through bounded concurrency verification



//Dropping should happen only on the very end on the overall process
//Back-pressure on DDataReplicator


https://jepsen.io/analyses/scylla-4.2-rc3

We allow clients to write to any node at any time — even when nodes are crashed or partitioned away. 
Whether a write is durable depends on whether it reaches a node which can store that row; whether the write is acknowledged to the client depends on whether the write's entity store data to local disk. 
Writes are isolated, long as they take place within a single partition.


Sharding allow us to avoid

Actually, kv workload allow us to them 
`Read-Write` (Unrepeatable read)
`Write-Read` (Dirty read) 
`Write-Write` (Lost update) 
(to guarantee the absence of conflicting updates)

### IDEAs

rocksdb (snapshot isolation)

Uses version vectors as seen table (for convergence) 


TODO: next
a) RocksDB      (https://github.com/thatdot/quine/blob/main/quine-rocksdb-persistor/src/main/scala/com/thatdot/quine/persistor/RocksDbPersistor.scala)
a) Flat-buffers (https://github.com/thatdot/quine/blob/main/project/FlatcPlugin.scala)


b) Star schema to colocate data (e.g. `Documents` / `Bundles`, `Tenant|Company` / `Users`) 

Q:why you would like to have them (B and D) as separate entities?
For Tenant/Users      - license with 
For Documents/Bundles -   


This talk is about Amazon’s SABLE DB. It has a basic DB-like API. 

It contains: 
Customer transaction data `Carts`, `Orders`.
Source data `Merchants`, `Offers`, `Products`. 
and intermediate aggregated data.

FlatBuffers

Streaming Repair (https://youtu.be/2dNtrCyhmXw?list=LL&t=1837) ???

https://habr.com/ru/company/odnoklassniki/blog/499316/
И, наконец, Streaming Repair — это пакетный фоновый процесс обхода и сравнения всех или какой-то части данных всех реплик.

c)
Key-Range Partitions
https://martinfowler.com/articles/patterns-of-distributed-systems/key-range-partitions.html#CalculatingPartitionSizeAndFindingTheMiddleKey

d)
https://github.com/softwaremill/vehicle-routing-problem-java
https://softwaremill.com/solving-vehicle-routing-problem-in-java/
http://download.openstreetmap.fr/extracts/

e) Collocate data (Start-Schema)

### Who’s Afraid Of Distributed Transactions? - Chuck Carman (Amazon)

https://emptysqua.re/blog/2022-hpts-notes/#whos-afraid-of-distributed-transactions---chuck-carman-amazon

Carman describes consensus view of “scale agnostic architecture” (what Carman doesn’t believe in anymore):

Follow patterns that scale, only do small things and partition small things by key.
Know your business and design for it, design for “right now”, make copies of data to avoid slow pages.
Apps need to scale, not DBs. This talk is about Amazon’s SABLE DB. It has a basic DB-like API. It contains customer transaction data (carts, orders), 
source data (merchants, offers, products), and intermediate aggregated data. 
A given Amazon product detail web page might query SABLE a hundred times.

SABLE prioritizes speed over correctness, “right now is more important than right”. It has small plentiful entities, everything is partitioned to maximize local operations. 
All writes are “publishes” in a pub sub architecture? 
Every published message includes a partition key to allow parallel processing. Messages trigger serverless functions, 
which can be arranged into complex pipelines including fan-in and fan-out. Functions execute small localized consistent transactions, but the system as a whole is eventually consistent?

So SABLE isn’t a DB, it’s an “application engine” or “environment”. Carman doesn’t believe in distributed DBs as an API for users anymore, he believes in higher level data apis for users, which may be backed by a DB as an implementation detail.

What he believes now:

High performance is specialization, business dictates what kind of correctness is required.
For high performance data, build an application engine. Don’t worry about generality, scale the business’s specific logic.
(I’m not clear how this contradicts the “scale agnostic architecture” above, but it’s interesting regardless.)



if [[ $i -gt 100 ]]
then
grpcurl -d '{"vehicleId":'${i}',"lon":1.1,"lat":1.1}' -plaintext 127.0.0.2:8080 com.rides.VehicleService/PostLocation;
#elif [[ $i -gt 100 ]]
else
grpcurl -d '{"vehicleId":'${i}',"lon":1.1,"lat":1.1}' -plaintext 127.0.0.1:8080 com.rides.VehicleService/PostLocation;
fi
#grpcurl -d '{"vehicleId":12,"version":11}' -plaintext 127.0.0.1:8080 com.rides.VehicleService/GetCoordinates;


Keep CALM and CRDT On
Excited to share our VLDB 2023 paper "Keep CALM and CRDT On" (w/ @conor_power23 and the Hydro group)! We explore the challenges in using CRDTs today and show how the CALM theorem lets us reason about when coordination-free CRDT queries are truly safe!
https://www.vldb.org/pvldb/vol16/p856-power.pdf


Merkle-CRDTs: Merkle-DAGs meet CRDTs.

https://github.com/ipfs/go-ds-crdt
https://arxiv.org/abs/2004.00107

https://arxiv.org/pdf/2004.00107.pdf

Merkle Search Trees
https://hal.inria.fr/hal-02303490/document

https://martin.kleppmann.com/papers/bft-crdt-papoc22.pdf


https://martinfowler.com/articles/patterns-of-distributed-systems/hybrid-clock.html

This means that you can create a network of nodes that use this datastore, 
and that each key-value pair written to it will automatically replicate to every other node. 
Updates can be published by any node. 
Network messages can be dropped, reordered, corrupted or duplicated. 
It is not necessary to know beforehand the number of replicas participating in the system. Replicas can join and leave at will, 
without informing any other replica. There can be network partitions but they are resolved as soon as connectivity 
is re-established between replicas.
                                                                                                    


https://www.microsoft.com/en-us/research/wp-content/uploads/2011/10/ConsistencyAndBaseballReport.pdf


https://twitter.com/ksshams/status/1620138308851597312?t=DJad5keF0iDxQz4gC-uZuw&s=03

SC reads have higher latencies, lower availability, and cost more - but worth it if you use them for the right reasons.


The SC reads are routed to the leader of a partition, which will give you the latest data while it's processing your request. 
However, all that can change as it serializes your response and some other request updates the underlying data.


------------
https://medium.com/@ifesdjeen/database-papers-anti-entropy-without-merkle-trees-deletes-without-tombstones-a47d2b1608f3
https://github.com/ricardobcl/DottedDB
https://github.com/ricardobcl/ServerWideClocks

------------------------


-----------------------
https://www.cockroachlabs.com/blog/global-tables-in-cockroachdb/

In brief, CockroachDB splits data into 512MB “ranges”. Each range is replicated at least 3 ways and the range’s replicas form a Raft consensus group. 
Writes need to be acknowledged by a majority of replicas before being applied.

At any point in time, one replica acts as the “leaseholder” for the range; 
the leaseholder needs to be part of all the write quorums and it is the only replica that can serve strongly consistent reads.
The other replicas can only serve historical, stale reads. Thus, from a client’s perspective, the latency of a read is dictated by the round-trip time to the leaseholder 
of the range being read. The latency of a write depends on the round-trip time to the leaseholder plus the time it takes to achieve consensus for that write.

------------------------------

https://medium.com/@ifesdjeen/database-papers-anti-entropy-without-merkle-trees-deletes-without-tombstones-a47d2b1608f3
https://github.com/ricardobcl/DottedDB
https://github.com/ricardobcl/ServerWideClocks

--------------------------------------------


Advanced Domain-Driven Design for Consistency in Distributed Data-Intensive Systems
https://dl.acm.org/doi/10.1145/3447865.3457969

Replicated bounded contexts
DDD ()

ExternalShardAllocationClient use cases: 
Colocate replicated bounded contexts: Tenant/Users, TaxPayer/Asset/Liability, or StartSchema(Fact/Dimensions)


-----------------------------------------------------
https://medium.com/@nishantparmar/distributed-system-design-patterns-2d20908fecfc

High-Water mark
Keep track of the last log entry on the leader, which has been successfully replicated to a quorum of followers. 
The index of this entry in the log is known as the High-Water Mark index. The leader exposes data only up to the high-water mark index.

Kafka: To deal with non-repeatable reads and ensure data consistency, Kafka brokers keep track of the high-water mark, 
which is the largest offset of a particular partition share. Consumers can see messages only until the high-water mark.

-----------------------------------------------------

IPA: Invariant-preserving Applications for Weakly-consistent Replicated Databases

Weak consistency models [33, 34, 48, 18, 29] allow replicas to diverge temporarily by accepting updates in a given replica and executing them locally without coordinating with other replicas.
After the execution finishes, updates are propagated asynchronously to other replicas. This provides low latency and high availability, but also allows the replicated system to expose anomalous states that are not allowed by strong consistency. 


---------------------------------------------------
Concise Server-Wide Causality Management for Eventually Consistent Data Stores

Serving reads 
    Any node upon receiving a read request can coordinate it, by asking the respective replica nodes for their local key version. 
When sufficient replies arrive, the coordinator discards obsolete versions and sends to the client the most recent (concurrent) 
version(s), w.r.t causality. It also sends the causal context for the value(s). Optionally, the coordinator can send the results 
back to replica nodes, if they have outdated versions (a process known as Read Repair).
Serving writes/deletes 
    Only replica nodes for the key being written can coordinate a write request, while non-replica nodes forward the request to a replica node. A coordinator node: (1) generates a new identifier for this write for the logical clock; (2) discards older versions according to the write’s context; (3) adds the new value to the local remaining set of concurrent versions; (4) propagates the result to the other replica nodes; (5) waits for configurable number of acks before replying to the client. Deletes are exactly the same, but omit step 3, since there is no new value.


--------------------------------------------------

https://emptysqua.re/blog/review-rex-replication/

For example, MongoDB guarantees causal consistency on secondaries(replicas) by reading at the majority-committed timestamp/durability version.
(https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#std-label-sessions)


Causal Consistency in MongoDB
If an operation logically depends on a preceding operation, there is a causal relationship between the operations. 
For example, a write operation that deletes all documents based on a specified condition and a subsequent read operation that verifies the delete operation have a causal relationship.

With causally consistent sessions, MongoDB executes causal operations in an order that respect their causal relationships, and clients observe results that are consistent with the causal relationships.


Client Sessions and Causal Consistency Guarantees
To provide causal consistency, MongoDB 3.6 enables causal consistency in client sessions. 
A causally consistent session denotes that the associated sequence of read operations with "majority" read concern and write operations with "majority" write concern 
have a causal relationship that is reflected by their ordering. 

Applications must ensure that only one thread at a time executes these operations in a client session.

-------------------------------------------------

https://github.com/sean-walsh/cloudstate-samples-bookings/blob/master/src/main/proto/wirelessmeshservice.proto
----------------------------------------------


Zero round trips per write



A)
Update(key = "x")
If it is not in memory we use `Read(Majority)`
1. Lookup the existing value (A)
2. val B = A.update() // inc the version and replicationState
3. val C = A merge B
4. Save(C)
5. Reply


B:
On Gossip receive
1. Read local value
2. Merge
3. writeAndStore
4. ReplyTo A (Gossip)

A
On Gossip receive
1. Read local value
2. Merge
3. writeAndStore

https://emptysqua.re/blog/consistency-aware-durability/