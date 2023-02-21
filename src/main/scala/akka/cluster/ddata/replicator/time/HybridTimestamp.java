package akka.cluster.ddata.replicator.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class HybridTimestamp implements Comparable<HybridTimestamp> {
  private final long wallClockTime;
  private final int ticks;

  public HybridTimestamp(long systemTime, int ticks) {
    this.wallClockTime = systemTime;
    this.ticks = ticks;
  }

  public static HybridTimestamp fromSystemTime(long systemTime) {
    return new HybridTimestamp(systemTime, -1); //initializing with -1 so that addTicks resets it to 0
  }

  public HybridTimestamp max(HybridTimestamp other) {
    if (this.getWallClockTime() == other.getWallClockTime()) {
      return this.getTicks() > other.getTicks() ? this : other;
    }
    return this.getWallClockTime() > other.getWallClockTime() ? this : other;
  }

  public long getWallClockTime() {
    return wallClockTime;
  }

  public HybridTimestamp addTicks(int ticks) {
    return new HybridTimestamp(wallClockTime, this.ticks + ticks);
  }

  public int getTicks() {
    return ticks;
  }

  @Override
  public int compareTo(HybridTimestamp other) {
    if (this.wallClockTime == other.wallClockTime) {
      return Integer.compare(this.ticks, other.ticks);
    }
    return Long.compare(this.wallClockTime, other.wallClockTime);
  }


  public LocalDateTime toDateTime() {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis()), ZoneOffset.UTC);
  }

  public long epochMillis() {
    //Compact timestamp as discussed in https://cse.buffalo.edu/tech-reports/2014-04.pdf.
    return (wallClockTime >> 16 << 16) | (ticks << 48 >> 48);
  }
}