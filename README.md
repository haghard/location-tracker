# Location tracker

Consistency-aware durability + Causal consistency

Eventual durability:

1) Master-acknowledged writes.
2) Async replication (Ensure independent and simultaneous progress on various locations) makes writes eventually durable, 
but enables only weak consistency due to data loss upon failure. If a failure arises before writes are made durable, they can be lost.


Cross-client monotonic read: Every time the system satisfies a query, the value that it returns is at least as fresh as the one returned by the previous query to any client.

Key idea: Shift the point of durability from writes to reads. In other words, data is replicated and persisted before reads are served.


`http GET 127.0.0.1:8079/orders/cluster/members`

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


`grpcurl -d '{"vehicleId":5,"lon":1,"lat":1.1}' -plaintext 127.0.0.1:8080 com.rides.VehicleService/PostLocation`


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


2. Replace  HybridTime
3. Bloom filter to reduce a number of actor recoveries to keys that don't exist.
4. Dependency tracking ()


