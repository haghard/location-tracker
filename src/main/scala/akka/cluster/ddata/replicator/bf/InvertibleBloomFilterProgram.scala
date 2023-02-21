package akka.cluster.ddata.replicator.bf

//runMain akka.cluster.ddata.replicator.bf.InvertibleBloomFilterProgram
object InvertibleBloomFilterProgram extends App {

  // final Keccak.Digest256 digest = new Keccak.Digest256();
  val f = java.security.MessageDigest.getInstance("SHA3-256")

  // Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0])

  // BloomFilter.create()

  val a = new InvertibleBloomFilter(100, f)
  val b = new InvertibleBloomFilter(100, f)
  a.add(1)
  a.add(2)
  a.add(3)
  a.add(4)
  a.add(5)

  b.add(1)
  b.add(3)
  b.add(7)
  b.add(8)
  b.add(9)

  val res  = a.subtract(b.getCells)
  val diff = a.decode(res) // res18: Array[java.util.List[E] forSome { type E }] = Array([5, 2, 4], [9, 8, 7])

  // A diff B (A has, B hasn't)
  // diff(0) //[5, 2, 4]

  /*diff._1.forEach { i =>
    println(s"$i  ${a.contains(i)} / ${b.contains(i)}")
  }

  // diff(1) //[9, 8, 7]
  diff._2.forEach { i =>
    println(s"$i  ${a.contains(i)} / ${b.contains(i)}")
  }*/

  /*

  val b1sb2 = Array.ofDim[Int](10)
  val b2sb1 = Array.ofDim[Int](20)

  (0 until 1999).foreach { _ =>
    val v = ThreadLocalRandom.current().nextInt(1, 1999)
    a.add(v)
    b.add(v)
  }

  for (i <- 0 until b1sb2.length) {
    val v = ThreadLocalRandom.current().nextInt(2001, 2900)
    a.add(v)
    b1sb2(i) = v
  }

  for (i <- 0 until b2sb1.length) {
    val v = ThreadLocalRandom.current().nextInt(3001, 3900)
    b.add(v)
    b2sb1(i) = v
  }

  util.Arrays.sort(b1sb2)
  util.Arrays.sort(b2sb1)

  val res  = a.subtract(b.getCells)
  val diff = a.decode(res)

  println("A has, B hasn't")
  diff(0).asInstanceOf[java.util.ArrayList[Int]].forEach { i =>
    println(s"$i  ${a.contains(i)} / ${b.contains(i)}")
  }

  println("B has, A hasn't")
  diff(1).asInstanceOf[java.util.ArrayList[Int]].forEach { i =>
    println(s"$i  ${a.contains(i)} / ${b.contains(i)}")
  }

  if (diff(0).size != b1sb2.length) println("error b1sb2")
  if (diff(1).size != b2sb1.length) println("error b2sb1")

  Collections.sort[java.lang.Integer](diff(0).asInstanceOf[java.util.ArrayList[java.lang.Integer]])
  Collections.sort[java.lang.Integer](diff(1).asInstanceOf[java.util.ArrayList[java.lang.Integer]])

  println("===========")

  for (i <- 0 until diff(0).size)
    println(b1sb2(i) + "," + diff(0).get(i))

  println("..........")

  for (i <- 0 until diff(1).size)
    println(b2sb1(i) + "," + diff(1).get(i))
   */

}
