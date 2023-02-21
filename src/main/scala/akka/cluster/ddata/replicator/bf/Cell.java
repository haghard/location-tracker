package akka.cluster.ddata.replicator.bf;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class Cell {
    private int count;
    private long idSum;
    private int hashSum;

    public void add(long id, int idHashValue) {
        count++;
        idSum ^= id;
        hashSum ^= idHashValue;
    }

    public Cell() {
    }

    public Cell(int count, long idSum, int hashSum) {
        this.count = count;
        this.idSum = idSum;
        this.hashSum= hashSum;
    }

    public boolean isPure(MessageDigest digestFunction) {
        if ((count == -1 || count == 1)
                && (InvertibleBloomFilter.genIdHash(String.valueOf(idSum).getBytes(StandardCharsets.UTF_8), digestFunction) == hashSum))
            return true;

        return false;
    }

    public void setCount(int count) { this.count = count; }

    public int getCount() {
        return count;
    }

    public void setIdSum(long idSum) { this.idSum = idSum; }

    public long getIdSum() {
        return idSum;
    }

    public void setHashSum(int hashSum) {
        this.hashSum = hashSum;
    }

    public int getHashSum() {
        return hashSum;
    }
}