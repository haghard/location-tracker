package akka.cluster.ddata.replicator.time;

public class Server  {

  HybridClock clock;

  public Server(/*HybridClockMVCCStore mvccStore*/) {
    this.clock = new HybridClock(new SystemClock());

    //new HybridTimestamp(1L, -1);

    //this.mvccStore = mvccStore;
  }


  public HybridTimestamp write(String key, String value, HybridTimestamp requestTimestamp) {
    //update own clock to reflect causality
    HybridTimestamp writeAtTimestamp = clock.tick(requestTimestamp);
    //mvccStore.put(key, writeAtTimestamp, value);
    return writeAtTimestamp;
  }
}
