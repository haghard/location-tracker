package akka.cluster.ddata.replicator.time;

public class SystemClock {
  public long now() {
    return System.nanoTime();
  }
}
