package com.rides.rocks

import akka.cluster.ddata.replicator.RocksDbSettings
import org.rocksdb.ReadOptions
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions

import java.io.File
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

//runMain com.rides.rocks.Program
object Program extends App {

  implicit class RichFutures[T](val inner: Future[T]) extends AnyVal {
    private def timeout(t: Long): Future[Boolean] = {
      val timer = new java.util.Timer(true)
      val p     = scala.concurrent.Promise[Boolean]()
      timer.schedule(
        new java.util.TimerTask {
          override def run() = {
            p.success(false)
            timer.cancel()
          }
        },
        t
      )
      p.future
    }
  }

  val (db, columnFamily) = RocksDbSettings.openRocksDB(
    new File(s"rocks/rocksdb-tmp").getAbsolutePath
  )

  def ins = Future {
    var i = 1000
    while (i < 1100) {
      try db.put(db.getDefaultColumnFamily, i.toString.getBytes, i.toString.getBytes)
      catch {
        case NonFatal(ex) =>
          println("Error A: " + ex.getMessage)
          throw ex
      }
      i = i + 1
    }
    println(i)
  }

  def a = Future {
    val snapshot = db.getSnapshot
    /*val transactionLogIterator = db.getUpdatesSince(0)
    transactionLogIterator.getBatch*/
    val read = new ReadOptions().setSnapshot(snapshot)
    val iter = db.newIterator(db.getDefaultColumnFamily, read)
    try {
      iter.seekToLast()
      println("A: " + new String(iter.key()))

      iter.seekToFirst()
      while (iter.isValid) {
        val key   = iter.key()
        val value = iter.value()
        Thread.sleep(ThreadLocalRandom.current().nextInt(10, 20))

        // println(new String(key))
        try db.put(columnFamily, key, value ++ "-a".getBytes)
        catch {
          case NonFatal(ex) =>
            println("Error A: " + ex.getMessage)
            throw ex
        }

        iter.next()
      }

      // println(snapshot.getSequenceNumber)
    } finally
      try iter.close()
      finally
        try read.close()
        finally
          db.releaseSnapshot(snapshot)

  }

  def b = Future {
    val snapshot = db.getSnapshot
    val read     = new ReadOptions().setSnapshot(snapshot)
    val iter     = db.newIterator(db.getDefaultColumnFamily, read)
    try {
      iter.seekToLast()
      println("B: " + new String(iter.key()))

      iter.seekToFirst()
      while (iter.isValid) {
        val key   = iter.key()
        val value = iter.value()
        Thread.sleep(ThreadLocalRandom.current().nextInt(10, 20))

        try db.put(columnFamily, key, value ++ "-b".getBytes)
        catch {
          case NonFatal(ex) =>
            println("Error B: " + ex.getMessage)
            throw ex
        }

        iter.next()
      }
      // println(snapshot.getSequenceNumber)
    } finally
      try iter.close()
      finally
        try read.close()
        finally
          db.releaseSnapshot(snapshot)

  }

  def c = Future {
    val it = db.newIterator(db.getDefaultColumnFamily)
    var i  = 30
    it.seekToFirst()
    while (it.isValid && i > 0) {
      println(new String(it.value()))
      i = i - 1
      it.next()
    }
  }

  def d = Future {
    val writeOptions = new WriteOptions().setDisableWAL(false).setSync(false)
    val writeBatch   = new WriteBatch()
    /*(0 to 10).foreach { i =>
      writeBatch.put(columnFamily, i.toString.getBytes, (i.toString + "_").getBytes)
    }*/

    if (writeBatch.data() != null) {
      println("writeBatch " + writeBatch.getDataSize + " / " + writeBatch.hasPut)
      db.write(writeOptions, writeBatch)
      writeBatch.close()
    }
  }

  // val f = Future.sequence(Seq(a, b)) // a.flatMap(_ => b)
  Await.result(d, Duration.Inf)
  Await.result(c, Duration.Inf)

  // https://github.com/haghard/scala-playbook/blob/c5a16eea97ac5d6a52e89eb4460d948a29e38695/src/test/scala/futures/FutureSpec.scala
  // val src = Source.fromIterator(()=> Iterator.range(1,10000))

  /*
  implicit val sys         = ActorSystem("demo")
  val (statusQ, statusSrc) = Source.queue[Int](8).preMaterialize()
  val (updateQ, updateSrc) = Source.queue[Int](8).preMaterialize()

  /*def flow: Flow[Int, Int, akka.NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        val preferred = builder.add(statusSrc)
        // val preferred = builder.add(Source.tick(interval, interval, zero))

        // 0 - preferred port
        // 1 - secondary port
        val merge = builder.add(MergePreferred[Int](1))
        preferred ~> merge.in(0)
        FlowShape(merge.in(1) /*.preferred*/, merge.out)
      }
    )
   */

  // If multiple have elements available, prefer the 'right' one when 'preferred' is 'true'.
  updateSrc
    .mergePreferred(statusSrc, true)
    .to(Sink.foreach(in => println(in.toString)))
    .run()

  (0 to 300).foreach { i =>
    updateQ.offer(-1)
    updateQ.offer(-1)
    updateQ.offer(-1)
    updateQ.offer(-1)

    statusQ.offer(i)
    Thread.sleep(75)
  }*/

  // Thread.sleep(10000)
  // sys.terminate()

}
