package akka.cluster.ddata.replicator.bf;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.cluster.ddata.crdt.protoc.CellPB;
import akka.cluster.ddata.crdt.protoc.CellsPB;
import scala.reflect.ClassTag;

import akka.cluster.ddata.crdt.protoc.CellPB;

/**
 * Source:
 * https://github.com/zsbreak/InvertibleBloomFilter/blob/master/src/test/java/com/zsbreak/InvertibleBloomFilter/BloomFilterTest.java
 *
 * <p>
 * https://medium.com/codechain/invertible-bloom-lookup-table-37600927cfbe
 * <p>
 * <p>
 * https://github.com/matheus23/rust-set-reconciliation
 * An implementation of "What’s the Difference? Efficient Set Reconciliation without Prior Context"
 * <p>
 * <p>
 * https://www.ics.uci.edu/~eppstein/pubs/EppGooUye-SIGCOMM-11.pdf
 * We now describe the Invertible Bloom Filter (IBF), which can simultaneously calculate Dif(A−B) and Dif(B−A) using O(d) space.
 * <p>
 * runMain akka.cluster.ddata.replicator.bf.InvertibleBloomFilter
 */

public class InvertibleBloomFilter {
    private int numOfHashFunc = 3; // number of hash functions (3/4)

    //the size of IBF is d * 1.5 (d, size of the set difference) that are required to successfully decode the IBF
    private int ibfSize;

    private Cell[] cells;

    public Cell[] getCells() {
        return cells;
    }

    private MessageDigest digestFunction;

    public InvertibleBloomFilter(int size, MessageDigest f) {
        this.digestFunction = f;
        this.ibfSize = size;
        this.cells = new Cell[size];
        for (int i = 0; i < cells.length; i++)
            cells[i] = new Cell();
    }

    public InvertibleBloomFilter(int size, MessageDigest f, Cell[] cells) {
        this.digestFunction = f;
        this.ibfSize = size;
        this.cells = cells;
    }

    public void add(long id) {
        byte[] sid = String.valueOf(id).getBytes(StandardCharsets.UTF_8);
        int[] hashes = genHashes(sid, numOfHashFunc);
        for (int hash : hashes) {
            int idx = Math.abs(hash % ibfSize);
            cells[idx].add(id, genIdHash(sid, digestFunction));
        }
        //System.out.println("ADD:" + id);
    }

    public boolean contains(long id) {
        byte[] sid = String.valueOf(id).getBytes(StandardCharsets.UTF_8);
        int[] hashes = genHashes(sid, numOfHashFunc);
        for (int hash : hashes) {
            Cell cell = cells[Math.abs(hash % ibfSize)];
            if (cell == null || cell.getCount() == 0) {
                return false;
            }
        }
        return true;
    }

    public Cell[] subtract(Cell[] b2) {
        Cell[] b1 = this.cells;
        Cell[] res = new Cell[b2.length];
        for (int i = 0; i < res.length; i++) {
            if (res[i] == null)
                res[i] = new Cell();

            res[i].setIdSum(b1[i].getIdSum() ^ b2[i].getIdSum());
            res[i].setHashSum(b1[i].getHashSum() ^ b2[i].getHashSum());
            res[i].setCount(b1[i].getCount() - b2[i].getCount());
        }
        return res;
    }

    public scala.Tuple2<List<Long>, List<Long>> decode(Cell[] other) {
        List<Long> additional = new ArrayList<>();
        List<Long> missing = new ArrayList<>();
        Queue<Integer> pureIdxList = new LinkedList<>();

        for (int i = 0; i < ibfSize; i++) {
            if (other[i].isPure(digestFunction))
                pureIdxList.add(i);
        }

        while (!pureIdxList.isEmpty()) {
            int i = pureIdxList.poll();
            if (!other[i].isPure(digestFunction))
                continue;

            long s = other[i].getIdSum();
            int c = other[i].getCount();

            if (c > 0) additional.add(s);
            else missing.add(s);

            byte[] sb = String.valueOf(s).getBytes(StandardCharsets.UTF_8);
            for (int hash : genHashes(sb, numOfHashFunc)) {
                int j = Math.abs(hash % ibfSize);
                other[j].setIdSum(other[j].getIdSum() ^ s);
                other[j].setHashSum(other[j].getHashSum() ^ genIdHash(sb, this.digestFunction));
                other[j].setCount(other[j].getCount() - c);
                if (other[j].isPure(digestFunction)) pureIdxList.add(j);
            }
        }

        for (int i = 0; i < other.length; i++) {
            if (other[i].getIdSum() != 0 || other[i].getHashSum() != 0 || other[i].getCount() != 0)
                //failed to decode
                return null;
        }

        return new scala.Tuple2<>(additional, missing);
    }

    public static int genIdHash(byte[] data, MessageDigest f) {
        int result = 0;
        int h = result;
        byte salt = 125;
        f.update(salt);
        byte[] digest = f.digest(data);
        for (int j = 0; j < 4; j++) {
            h <<= 8;
            h |= ((int) digest[j]) & 0xFF;
        }
        result = h;
        return result;
    }

    private int[] genHashes(byte[] data, int hashes) {
        int[] result = new int[hashes];

        int k = 0;
        byte salt = 0;
        while (k < hashes) {
            digestFunction.update(salt);
            salt++;

            byte[] digest = digestFunction.digest(data);
            for (int i = 0; i < digest.length / 4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i * 4); j < (i * 4) + 4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    public akka.cluster.ddata.crdt.protoc.CellsPB toProto() {
        scala.collection.mutable.ReusableBuilder<CellPB, scala.collection.immutable.Vector<CellPB>> b = scala.collection.immutable.Vector.newBuilder();

        b.sizeHint(cells.length);
        for (int i = 0; i < cells.length; i++) {
            Cell c = cells[i];
            //arr.update(i, new akka.cluster.ddata.crdt.protoc.CellPB(c.getCount(), c.getIdSum(), c.getHashSum()));
            b.addOne(new CellPB(c.getCount(), c.getIdSum(), c.getHashSum()));
        }
        return CellsPB.apply(b.result());
    }

    public static InvertibleBloomFilter from(akka.util.ByteString bs, MessageDigest digestFunction) {
        CellsPB agg = (CellsPB) CellsPB.parseFrom(bs.toArray());

        scala.collection.immutable.Seq<CellPB> cells = agg.cells();
        Cell[] src = new Cell[cells.length()];

        for (int i = 0; i < cells.length(); i++) {
            CellPB cell = cells.apply(i);
            src[i] = new Cell(cell.count(), cell.idSum(), cell.hashSum());
        }
        return new InvertibleBloomFilter(src.length, digestFunction, src);
    }

    public static void main(String[] data) throws NoSuchAlgorithmException {
        Random r = new Random();

        //final Keccak.Digest256 digest =new Keccak.Digest256();
        MessageDigest f = MessageDigest.getInstance("SHA3-256"); //SHA3-256, MD5

        InvertibleBloomFilter b1 = new InvertibleBloomFilter(200, f);
        InvertibleBloomFilter b3 = new InvertibleBloomFilter(200, f);


        for (int i = 0; i < 1_500_000; i++) {
            long val = r.nextLong();
            b1.add(val);
            b3.add(val);
        }

        long b1sb2[] = new long[55]; //10, 5, 20|7
        long b2sb1[] = new long[65]; //20     30|

        for (int i = 0; i < b1sb2.length; i++) {
            long val = r.nextLong(); //99999999;
            b1.add(val);
            b1sb2[i] = val;
        }
        for (int i = 0; i < b2sb1.length; i++) {
            long val = r.nextLong();
            b3.add(val);
            b2sb1[i] = val;
        }

        Arrays.sort(b1sb2);
        Arrays.sort(b2sb1);

        akka.util.ByteString b = akka.util.ByteString.fromArrayUnsafe(b3.toProto().toByteArray());
        InvertibleBloomFilter b2 = InvertibleBloomFilter.from(b, f);

        long start = System.currentTimeMillis();
        Cell[] res = b1.subtract(b2.getCells());
        scala.Tuple2<List<Long>, List<Long>> diff = b1.decode(res);
        System.out.println("Latency in mils " + (System.currentTimeMillis() - start)); //506490

        if (diff != null) {

            //assert (diff != null);
            if (diff._1.size() != b1sb2.length) System.out.println("error b1sb2");
            if (diff._2.size() != b2sb1.length) System.out.println("error b2sb1");

            Collections.sort(diff._1);
            Collections.sort(diff._2);

            System.out.println("===========");

            for (int i = 0; i < diff._1.size(); i++) {
                long j = diff._1.get(i);
                System.out.println(b1sb2[i] + "," + diff._1.get(i)); // + " " + b1.contains(j));
            }
            System.out.println("..........");
            for (int i = 0; i < diff._2.size(); i++) {
                System.out.println(b2sb1[i] + "," + diff._2.get(i));
            }
        } else {
            System.out.println("Failed ..");
        }
    }
}