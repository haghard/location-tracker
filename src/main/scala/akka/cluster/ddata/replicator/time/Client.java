package akka.cluster.ddata.replicator.time;

public class Client {
  final HybridClock clock = new HybridClock(new SystemClock());

  /*public void write() {
    HybridTimestamp server1WrittenAt = server1.write("name", "Alice", clock.now());
    clock.tick(server1WrittenAt);

    HybridTimestamp server2WrittenAt = server2.write("title", "Microservices", clock.now());

    //assertTrue(server2WrittenAt.compareTo(server1WrittenAt) > 0);
  }*/
}
