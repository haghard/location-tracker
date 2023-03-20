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
