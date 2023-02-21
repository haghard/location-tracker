package akka.cluster.ddata.replicator

import org.rocksdb._
import org.rocksdb.util.SizeUnit

object RocksDbSettings {

  final case class Compaction(initialFileSize: Long, blockSize: Long, writeRateLimit: Option[Long])

  object Compaction {

    import SizeUnit._

    val SSD: Compaction = Compaction(
      initialFileSize = 64 * MB,
      blockSize = 16 * KB,
      writeRateLimit = None
    )

    val HDD: Compaction = Compaction(
      initialFileSize = 256 * MB,
      blockSize = 64 * KB,
      writeRateLimit = Some(16 * MB)
    )
  }

  // TODO All options should become part of configuration
  val MaxOpenFiles: Int           = 512
  val BytesPerSync: Long          = 1 * SizeUnit.MB
  val MemoryBudget: Long          = 128 * SizeUnit.MB
  val WriteBufferMemoryRatio: Int = 2
  val BlockCacheMemoryRatio: Int  = 3
  // val CPURatio: Int = 3

  def databaseOptionsForBudget(compaction: Compaction, memoryBudgetPerCol: Long): DBOptions = {
    val options = new DBOptions()
      .setUseFsync(false)
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)
      .setMaxOpenFiles(MaxOpenFiles)
      .setKeepLogFileNum(1)
      .setBytesPerSync(BytesPerSync)
      .setDbWriteBufferSize(memoryBudgetPerCol / WriteBufferMemoryRatio)
      .setIncreaseParallelism(4) // because of Sink.foreachAsync(4) { msg: RMsg =>
    // .setIncreaseParallelism(Math.max(1, Runtime.getRuntime.availableProcessors() / CPURatio))

    compaction.writeRateLimit match {
      case Some(rateLimit) => options.setRateLimiter(new RateLimiter(rateLimit))
      case None            => options
    }
  }

  def columnOptionsForBudget(
    compaction: Compaction,
    memoryBudgetPerCol: Long
  ): ColumnFamilyOptions = {
    import scala.jdk.CollectionConverters._
    (new ColumnFamilyOptions)
      .setLevelCompactionDynamicLevelBytes(true)
      // .setMemTableConfig(new SkipListMemTableConfig())
      .setTableFormatConfig(
        new BlockBasedTableConfig()
          .setBlockSize(compaction.blockSize)
          .setBlockCache(new LRUCache(MemoryBudget / BlockCacheMemoryRatio))
          .setCacheIndexAndFilterBlocks(true)
          .setPinL0FilterAndIndexBlocksInCache(true)
      )
      .optimizeLevelStyleCompaction(memoryBudgetPerCol)
      .setTargetFileSizeBase(compaction.initialFileSize)
      .setCompressionPerLevel(Nil.asJava)
  }

  /** https://github.com/alephium/alephium/blob/master/benchmark/src/main/scala/org/alephium/benchmark/RocksDBBench.scala
    */
  private def loadRocksDbLibrary(): Boolean =
    try {
      RocksDB.loadLibrary()
      true
    } catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError => false
    }

  def openRocksDB(filePath: String): (RocksDB, ColumnFamilyHandle) = {
    loadRocksDbLibrary()

    /*val dbOpts = new DBOptions()
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)
      .setMaxOpenFiles(MaxOpenFiles)
      .setKeepLogFileNum(1)
      .setBytesPerSync(BytesPerSync)
      // .setDbWriteBufferSize(memoryBudgetPerCol / WriteBufferMemoryRatio)
      // .setIncreaseParallelism(totalThreads)
      // .setIncreaseParallelism(Math.max(1, Runtime.getRuntime.availableProcessors() / CPURatio))
      .setIncreaseParallelism(4) // because of Sink.foreachAsync(4) { msg: RMsg =>
     */

    val memoryBudgetPerCol = 128 * SizeUnit.MB
    val dbOpts             = databaseOptionsForBudget(Compaction.SSD, memoryBudgetPerCol)
    val columnFamilyOpts   = columnOptionsForBudget(Compaction.SSD, memoryBudgetPerCol)

    /*val columnFamilyOpts = new ColumnFamilyOptions()
      .optimizeUniversalStyleCompaction()
      .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR)*/

    val default = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOpts)

    // Make the column families
    val columnFamilyDesc    = java.util.Arrays.asList(default)
    val columnFamilyHandles = new java.util.ArrayList[ColumnFamilyHandle]()

    val db = RocksDB.open(dbOpts, filePath, columnFamilyDesc, columnFamilyHandles)
    (db, columnFamilyHandles.get(0))
  }
}
