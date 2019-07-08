package org.apache.spark.sql.redis

import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.redislabs.provider.redis.rdd.Keys._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.PipelineUtils._
import com.redislabs.provider.redis.util.{KryoUtils, Logging, StopWatch}
import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig, RedisDataTypeHash, RedisDataTypeString, RedisEndpoint, RedisNode, toRedisContext}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.redis.RedisSourceRelation._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import redis.clients.jedis.{Pipeline, PipelineBase, Protocol}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}


/** A singleton object that controls the parallelism on a Single Executor JVM
  */
object ThreadedConcurrentContext {

  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._

  /** Wraps a code block in a Future and returns the future */
  def executeAsync[T](f: => T)(implicit ec: ExecutionContext): Future[T] = {
    Future(f)(ec)
  }

  /** Awaits an entire sequence of futures and returns an iterator. This will
    * wait for all futures to complete before returning **/
  def awaitAll[T](it: Iterator[Future[T]], timeout: Duration = Inf)(implicit ec: ExecutionContext): Iterator[T] = {
    Await.result(Future.sequence(it), timeout)
  }

  /** Awaits only a set of elements at a time. Instead of waiting for the entire batch
    * to finish waits only for the head element before requesting the next future */
  def awaitSliding[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf)(implicit ec: ExecutionContext): Iterator[T] = {
    val slidingIterator = it.sliding(batchSize - 1).withPartial(true) //Our look ahead (hasNext) will auto start the nth future in the batch
    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    initIterator.map(futureBatch => Await.result(futureBatch.head, timeout)) ++
      tailIterator.flatMap(lastBatch => Await.result(Future.sequence(lastBatch), timeout))
  }
}


class RowHandler(threadPoolSize: Int, serializerArray: Array[RowSerializer], kryoArray: Array[Kryo], outputArray: Array[Output], bufferArray: Array[Array[Byte]], pipelineArray: Array[Pipeline], row: Row, redisKey: Array[Byte]) extends Runnable {

  val threadId: Int = Thread.currentThread().getId.toInt % threadPoolSize

  val output: Output = outputArray(threadId)

  //val pipeline: Pipeline = pipelineArray(threadId)

  val kryo: Kryo = kryoArray(threadId)

  val buffer: Array[Byte] = bufferArray(threadId)


  val rowSerializer = serializerArray(threadId)

  def run() {
    output.setPosition(0)
    rowSerializer.write(kryo, output, row)
    val bytes = buffer.slice(0, output.position())
    //    pipeline.set(redisKey, buffer.slice(0, output.position()))
  }
}

class RedisSourceRelation(override val sqlContext: SQLContext,
                          parameters: Map[String, String],
                          userSpecifiedSchema: Option[StructType])
  extends BaseRelation
    with InsertableRelation
    with PrunedFilteredScan
    with Serializable
    with Logging {


  private implicit val redisConfig: RedisConfig = {
    new RedisConfig(
      if ((parameters.keySet & Set("host", "port", "auth", "dbNum", "timeout")).isEmpty) {
        new RedisEndpoint(sqlContext.sparkContext.getConf)
      } else {
        val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
        val port = parameters.get("port").map(_.toInt).getOrElse(Protocol.DEFAULT_PORT)
        val auth = parameters.getOrElse("auth", null)
        val dbNum = parameters.get("dbNum").map(_.toInt).getOrElse(Protocol.DEFAULT_DATABASE)
        val timeout = parameters.get("timeout").map(_.toInt).getOrElse(Protocol.DEFAULT_TIMEOUT)
        RedisEndpoint(host, port, auth, dbNum, timeout)
      }
    )
  }

  implicit private val readWriteConfig: ReadWriteConfig = {
    val global = ReadWriteConfig.fromSparkConf(sqlContext.sparkContext.getConf)
    // override global config with dataframe specific settings
    global.copy(
      scanCount = parameters.get(SqlOptionScanCount).map(_.toInt).getOrElse(global.scanCount),
      maxPipelineSize = parameters.get(SqlOptionMaxPipelineSize).map(_.toInt).getOrElse(global.maxPipelineSize)
    )
  }

  logInfo(s"Redis config initial host: ${redisConfig.initialHost}")

  @transient private val sc = sqlContext.sparkContext
  /** parameters (sorted alphabetically) **/
  private val filterKeysByTypeEnabled = parameters.get(SqlOptionFilterKeysByType).exists(_.toBoolean)
  private val inferSchemaEnabled = parameters.get(SqlOptionInferSchema).exists(_.toBoolean)
  private val iteratorGroupingSize = parameters.get(SqlOptionIteratorGroupingSize).map(_.toInt)
    .getOrElse(SqlOptionIteratorGroupingSizeDefault)
  private val keyColumn = parameters.get(SqlOptionKeyColumn)
  private val keyName = keyColumn.getOrElse("_id")
  private val keysPatternOpt: Option[String] = parameters.get(SqlOptionKeysPattern)
  private val numPartitions = parameters.get(SqlOptionNumPartitions).map(_.toInt)
    .getOrElse(SqlOptionNumPartitionsDefault)
  private val persistenceModel = parameters.getOrDefault(SqlOptionModel, SqlOptionModelHash)
  private val persistence = RedisPersistence(persistenceModel)
  private val tableNameOpt: Option[String] = parameters.get(SqlOptionTableName)
  private val ttl = parameters.get(SqlOptionTTL).map(_.toInt).getOrElse(0)
  private val logInfoVerbose = parameters.get(SqlOptionLogInfoVerbose).exists(_.toBoolean)
  private val blockSize = parameters.get(SqlOptionBlockSize).map(_.toInt).getOrElse(SqlOptionBlockSizeDefault)
  private val kryoBufferSize = parameters.get(SqlOptionRedisKryoSerializerBufferSizeKB).map(_.toInt).getOrElse(SqlOptionRedisKryoSerializerBufferSizeKBDefault) * 1024
  private val rowThreadPoolSize = parameters.get(SqlOptionRowThreadPoolSize).map(_.toInt).getOrElse(SqlOptionRowThreadPoolSizeDefault)


  /**
    * redis key pattern for rows, based either on the 'keys.pattern' or 'table' parameter
    */
  private val dataKeyPattern = keysPatternOpt
    .orElse(tableNameOpt.map(tableName => tableDataKeyPattern(tableName)))
    .getOrElse {
      val msg = s"Neither '$SqlOptionKeysPattern' or '$SqlOptionTableName' option is set."
      throw new IllegalArgumentException(msg)
    }

  logInfo(f"Redis block size: ${blockSize}")
  logInfo(f"Redis persistence model: ${persistenceModel}")
  logInfo(s"Redis config maxPipelineSize: ${readWriteConfig.maxPipelineSize}")
  logInfo(f"Redis kryoBufferSize of: ${kryoBufferSize} bytes")
  logInfo(f"Redis rowThreadPoolSize of: ${rowThreadPoolSize} threads")


  /**
    * Support key column extraction from Redis prefix pattern. Otherwise,
    * return Redis key unmodified.
    */
  private val keysPrefixPattern =
    if (dataKeyPattern.endsWith("*") && dataKeyPattern.count(_ == '*') == 1) {
      dataKeyPattern
    } else {
      ""
    }
  /**
    * Will be filled while saving data to Redis or reading from Redis.
    */
  @volatile private var currentSchema: StructType = _

  // check specified parameters
  if (tableNameOpt.isDefined && keysPatternOpt.isDefined) {
    throw new IllegalArgumentException(s"Both options '$SqlOptionTableName' and '$SqlOptionTableName' are set. " +
      s"You should only use either one.")
  }

  override def schema: StructType = {
    if (currentSchema == null) {
      currentSchema = userSpecifiedSchema.getOrElse {
        if (inferSchemaEnabled) inferSchema() else loadSchema()
      }
    }
    currentSchema
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val stopWatch = new StopWatch()

    val schema = userSpecifiedSchema.getOrElse(data.schema)
    // write schema, so that we can load dataframe back
    currentSchema = saveSchema(schema)
    if (overwrite) {
      // truncate the table
      sc.fromRedisKeyPattern(dataKeyPattern).foreachPartition { partition =>
        groupKeysByNode(redisConfig.hosts, partition).foreach { case (node, keys) =>
          val conn = node.connect()
          foreachWithPipeline(conn, keys) { (pipeline, key) =>
            (pipeline: PipelineBase).del(key) // fix ambiguous reference to overloaded definition
          }
          conn.close()
        }
      }
      logInfo(f"Time taken to delete dataframe (SaveMode.Overwrite): ${stopWatch.getTimeSec()}%.3f sec")
    }
    data.foreachPartition { partition =>
    persistenceModel match {
      case SqlOptionModelHash => writeRows(partition)
      case SqlOptionModelBinary => writeRows(partition)
      case SqlOptionModelBlock => {
          implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(rowThreadPoolSize))

          val partId = TaskContext.getPartitionId()
          val table = tableName()
          // assuming that the partition keys belong all to the same redis instance via :{partition_id}:
          val conn = redisConfig.connectionForKey(dataKey(table, partId))
          val pipeline = conn.pipelined()

          val bufferArray: Array[Array[Byte]] = new Array[Array[Byte]](rowThreadPoolSize)
          val outputArray: Array[Output] = new Array[Output](rowThreadPoolSize)
          val pipelineArray: Array[Pipeline] = new Array[Pipeline](rowThreadPoolSize)
          val pipelinePos: Array[Integer] = new Array[Integer](rowThreadPoolSize)
          val kryoArray: Array[Kryo] = new Array[Kryo](rowThreadPoolSize)
          val serializerArray: Array[RowSerializer] = new Array[RowSerializer](rowThreadPoolSize)

          for (threadNum <- 0 until rowThreadPoolSize){
            bufferArray(threadNum) = new Array[Byte](kryoBufferSize)
            outputArray(threadNum) = new Output(bufferArray(threadNum))
            val kryo = KryoUtils.Pool.borrow()
            val rowSerializer = new RowSerializer(schema)
            serializerArray(threadNum) = rowSerializer
            kryo.addDefaultSerializer(RowClass, rowSerializer)
            kryo.register(RowClass)
            kryo.register(GenericRowClass)
            kryoArray(threadNum) = kryo
            pipelineArray(threadNum) = conn.pipelined()
            pipelinePos(threadNum) = 0
          }

          val resultSet: Iterator[Future[Array[Byte]]] = partition.map(row => {
            val threadId: Int = Thread.currentThread().getId.toInt % rowThreadPoolSize

            val result = ThreadedConcurrentContext.executeAsync(workerSerializer(serializerArray(threadId), outputArray(threadId), kryoArray(threadId), bufferArray(threadId), row))
            result
          })

          ThreadedConcurrentContext.awaitSliding(resultSet).foreach(valueBytes => {
            val threadId: Int = Thread.currentThread().getId.toInt % rowThreadPoolSize
            pipelinePos(threadId) =  pipelinePos(threadId) + 1
            ThreadedConcurrentContext.executeAsync(workerRedis(pipeline, dataKey(table, partId).getBytes(), valueBytes,  pipelinePos(threadId), readWriteConfig.maxPipelineSize ))
          } )

          for (threadNum <- 0 until rowThreadPoolSize) {
            KryoUtils.Pool.release(kryoArray(threadNum))
          }
          conn.close()
        logInfo(f"Time taken to insert data: ${stopWatch.getTimeSec()}%.3f sec")

      }

      }
    }


  }

  private def workerSerializer(rowSerializer: RowSerializer, output: Output, kryo: Kryo, buffer: Array[Byte], row: Row): Array[Byte] = {
    output.setPosition(0)
    rowSerializer.write(kryo, output, row)
    buffer.slice(0, output.position())
  }

  private def workerRedis(pipeline: Pipeline, keyArray: Array[Byte], valueArray: Array[Byte], pipelinePos : Int, pipelineMax : Int): Unit = {
    pipeline.set(keyArray, valueArray)
    if (pipelinePos >= pipelineMax){
      pipeline.sync()
    }
  }

  /**
    * write schema to redis
    */
  private def saveSchema(schema: StructType): StructType = {
    val key = schemaKey(tableName())
    logInfo(s"saving schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val conn = schemaNode.connect()
    val schemaBytes = SerializationUtils.serialize(schema)
    conn.set(key.getBytes, schemaBytes)
    conn.close()
    schema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo("build scan")
    val scanStopWatch = new StopWatch()

    val keysRdd = sc.fromRedisKeyPattern(dataKeyPattern, partitionNum = numPartitions)
    if (logInfoVerbose) {
      val partId = TaskContext.getPartitionId()
      logInfo(f"SCAN TIME: ${scanStopWatch.getTimeSec()}%.3f sec")
    }
    // requiredColumns is empty for .count() operation.
    // for persistence model where there is no grouping - no need to read actual values
    if (requiredColumns.isEmpty &&
      (persistenceModel == SqlOptionModelHash || persistenceModel == SqlOptionModelBinary)) {
      keysRdd.map { _ =>
        new GenericRow(Array[Any]())
      }
    } else {
      // filter schema columns, it should be in the same order as given 'requiredColumns'
      val requiredSchema = {
        val fieldsMap = schema.fields.map(f => (f.name, f)).toMap
        val requiredFields = requiredColumns.map { c =>
          fieldsMap(c)
        }
        StructType(requiredFields)
      }
      val keyType =
        if (persistenceModel == SqlOptionModelBinary) {
          RedisDataTypeString
        } else {
          RedisDataTypeHash
        }

      keysRdd.mapPartitions { partition =>
        val partitionStopWatch = new StopWatch()
        // grouped iterator to only allocate memory for a portion of rows
        val groupRow = partition.grouped(iteratorGroupingSize).flatMap { batch =>
          val stopWatch = new StopWatch()
          var marker = 0.0
          val group = groupKeysByNode(redisConfig.hosts, batch)

          if (logInfoVerbose) {
            val partId = TaskContext.getPartitionId()
            logInfo(f"Time taken to group by node ${batch.size} values/blocks in partition $partId: ${stopWatch.getTimeSec()}%.3f sec")
          }
          marker = stopWatch.getTimeSec()
          val rows = group.flatMap { case (node, keys) =>
            scanRows(node, keys, keyType, requiredSchema, requiredColumns)
          }
          if (logInfoVerbose) {
            val partId = TaskContext.getPartitionId()
            logInfo(f"Time taken to read ${batch.size} values/blocks in partition $partId: ${stopWatch.getTimeSec() - marker}%.3f sec")
          }
          rows
        }
        if (logInfoVerbose) {
          val partId = TaskContext.getPartitionId()
          logInfo(f"PARTITION READ with ID $partId: ${partitionStopWatch.getTimeSec()}%.3f sec")
        }
        groupRow

      }
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  /**
    * @return true if no data exists in redis
    */
  def nonEmpty: Boolean = {
    !isEmpty
  }

  /**
    * @return true if data exists in redis
    */
  def isEmpty: Boolean = {
    val stopWatch = new StopWatch()
    val isEmpty = sc.fromRedisKeyPattern(dataKeyPattern, partitionNum = numPartitions).isEmpty()
    logInfo(f"Time taken to check if keys exist for pattern $dataKeyPattern: ${stopWatch.getTimeSec()}%.3f sec")
    isEmpty
  }

  private def writeBlocks(rowThreadPool: ExecutorService, partition: Iterator[Row], schema: StructType): Unit = {
    val stopWatchWriteBlocks = new StopWatch()

    val partId = TaskContext.getPartitionId()

    val table = tableName()
    var currentRedisKey = dataKey(table, partId)

    // assuming that the partition keys belong all to the same redis instance via :{partition_id}:
    val conn = redisConfig.connectionForKey(currentRedisKey)

    val bufferArray: Array[Array[Byte]] = new Array[Array[Byte]](rowThreadPoolSize)
    val outputArray: Array[Output] = new Array[Output](rowThreadPoolSize)
    val pipelineArray: Array[Pipeline] = new Array[Pipeline](rowThreadPoolSize)
    val pipelinePos: Array[Integer] = new Array[Integer](rowThreadPoolSize)
    val kryoArray: Array[Kryo] = new Array[Kryo](rowThreadPoolSize)
    val serializerArray: Array[RowSerializer] = new Array[RowSerializer](rowThreadPoolSize)

    for (threadNum <- 0 until rowThreadPoolSize) {
      bufferArray(threadNum) = new Array[Byte](kryoBufferSize)
      outputArray(threadNum) = new Output(bufferArray(threadNum))
      pipelineArray(threadNum) = conn.pipelined()
      val kryo = KryoUtils.Pool.borrow()
      val rowSerializer = new RowSerializer(schema)
      serializerArray(threadNum) = rowSerializer
      kryo.addDefaultSerializer(RowClass, rowSerializer)
      kryo.register(RowClass)
      kryo.register(GenericRowClass)
      kryoArray(threadNum) = kryo
      pipelinePos(threadNum) = 0
    }

    logInfo(f"writeBlocks setup time forPartion ID: ${partId}  ${stopWatchWriteBlocks.getTime()} msec")
    //    logInfo(f"Partion ID: ${partId} Using output buffer length of ${buffer.length} Bytes")

    var marker: Long = stopWatchWriteBlocks.getTime()

    for (row <- partition) {
      rowThreadPool.execute(new RowHandler(rowThreadPoolSize, serializerArray, kryoArray, outputArray, bufferArray, pipelineArray, row, dataKey(table, partId).getBytes))
    }

    logInfo(f"writeBlocks threadPool timeforPartion ID: ${partId}  ${stopWatchWriteBlocks.getTime() - marker} msec")

    for (threadNum <- 0 until rowThreadPoolSize) {
      pipelineArray(threadNum).sync()
    }

    for (threadNum <- 0 until rowThreadPoolSize) {
      KryoUtils.Pool.release(kryoArray(threadNum))
    }

    conn.close()

    if (logInfoVerbose) {
      logInfo(f"writeBlocks ALL blocks: ${stopWatchWriteBlocks.getTime()}%.3f sec")
    }

  }

  private def writeRows(partition: Iterator[Row]): Unit = {
    // grouped iterator to only allocate memory for a portion of rows
    val partId = TaskContext.getPartitionId()
    partition.grouped(iteratorGroupingSize).foreach { batch =>
      val stopWatch = new StopWatch()
      // the following can be optimized to not create a map
      val rowsWithKey: Map[String, Row] = batch.map(row => dataKeyId(row, partId) -> row).toMap
      groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
        val conn = node.connect()
        foreachWithPipeline(conn, keys) { (pipeline, key) =>
          val row = rowsWithKey(key)
          val encodedRow = persistence.encodeRow(keyName, row)
          persistence.save(pipeline, key, encodedRow, ttl)
        }
        conn.close()
      }
      if (logInfoVerbose) {
        logInfo(f"Time taken to write ${batch.size} rows in partition $partId: ${stopWatch.getTimeSec()}%.3f sec")
      }
    }
  }

  /**
    * @return redis key for the row
    */
  private def dataKeyId(row: Row, partitionId: Integer): String = {
    val id = keyColumn.map(id => row.getAs[Any](id)).map(_.toString).getOrElse(uuid())
    dataKey(tableName(), partitionId, id)
  }

  /**
    * @return table name
    */
  private def tableName(): String = {
    tableNameOpt.getOrElse(throw new IllegalArgumentException(s"Option '$SqlOptionTableName' is not set."))
  }

  /**
    * infer schema from a random redis row
    */
  private def inferSchema(): StructType = {
    if (persistenceModel != SqlOptionModelHash) {
      throw new IllegalArgumentException(s"Cannot infer schema from model '$persistenceModel'. " +
        s"Currently, only '$SqlOptionModelHash' is supported")
    }
    val keys = sc.fromRedisKeyPattern(dataKeyPattern)
    if (keys.isEmpty()) {
      throw new IllegalStateException("No key is available")
    } else {
      val firstKey = keys.first()
      val node = getMasterNode(redisConfig.hosts, firstKey)
      withConnection(node.connect()) { conn =>
        val results = conn.hgetAll(firstKey).asScala.toSeq :+ keyName -> firstKey
        val fields = results.map(kv => StructField(kv._1, StringType)).toArray
        StructType(fields)
      }
    }
  }

  /**
    * read schema from redis
    */
  private def loadSchema(): StructType = {
    val key = schemaKey(tableName())
    logInfo(s"loading schema $key")
    val schemaNode = getMasterNode(redisConfig.hosts, key)
    val conn = schemaNode.connect()
    val schemaBytes = conn.get(key.getBytes)
    if (schemaBytes == null) {
      throw new IllegalStateException(s"Unable to read dataframe schema by key '$key'. " +
        s"If dataframe was not persisted by Spark, provide a schema explicitly with .schema() or use 'infer.schema' option. ")
    }
    val schema = SerializationUtils.deserialize[StructType](schemaBytes)
    conn.close()
    schema
  }

  /**
    * read rows from redis
    */
  private def scanRows(node: RedisNode, keys: Seq[String], keyType: String, requiredSchema: StructType,
                       requiredColumns: Seq[String]): Seq[Row] = {
    withConnection(node.connect()) { conn =>
      val filteredKeys =
        if (filterKeysByTypeEnabled) {
          val keyTypes = mapWithPipeline(conn, keys) { (pipeline, key) =>
            pipeline.`type`(key)
          }
          keys.zip(keyTypes).filter(_._2 == keyType).map(_._1)
        } else {
          keys
        }

      def readRows(): Seq[Row] = {
        val pipelineValues = mapWithPipeline(conn, filteredKeys) { (pipeline, key) =>
          persistence.load(pipeline, key, requiredColumns)
        }
        filteredKeys.zip(pipelineValues).map { case (key, value) =>
          val keyMap = keyName -> tableKey(keysPrefixPattern, key)
          persistence.decodeRow(keyMap, value, requiredSchema, requiredColumns)
        }
      }

      // TODO: optimize .count() operation
      def readBlocks(): Seq[Row] = {
        var finalRowsList: List[Row] = List()
        val kryo = KryoUtils.Pool.borrow()
        val rowSerializer = new RowSerializer(schema)
        kryo.addDefaultSerializer(RowClass, rowSerializer)
        kryo.register(RowClass)
        kryo.register(GenericRowClass)

        var numCols: Integer = schema.size


        // TODO:
        def project(fullRow: Row, requiredSchema: StructType): Row = {
          val cols = requiredSchema.fieldNames.map(fieldName => fullRow.getAs[Any](fieldName))
          new GenericRowWithSchema(cols, schema)
        }

        val sw1 = new StopWatch()
        val pipelineValues: Seq[Array[Byte]] = mapWithPipeline(conn, filteredKeys) { (pipeline, key) =>
          pipeline.get(key.getBytes)
        }.asInstanceOf[Seq[Array[Byte]]]
        logInfo(f"Time taken to read bytes from Redis: ${sw1.getTimeSec()}%.3f sec")
        val sw2 = new StopWatch()
        val persistedSchema = schema
        val projectionRequired = persistedSchema != requiredSchema
        pipelineValues.foreach { bytes =>
          if (bytes.length > 0) {
            val swKryo = new StopWatch()
            val input = new Input(bytes)
            input.setPosition(0)
            var numRows: Integer = input.readInt()
            logInfo(f"reading $numRows rows from redis with available bytes ${input.available}")
            for (rowNumber <- 0 until numRows) {
              //              logInfo(f"reading row $rowNumber")
              val deserializedRow = rowSerializer.read(kryo, input, RowClass)
              val row = if (projectionRequired) {
                project(deserializedRow, requiredSchema)
              } else {
                deserializedRow
              }
              finalRowsList = row :: finalRowsList
            }
            input.close()
            logInfo(f"Time taken to deserialize blocks to objects (Kryo time): ${swKryo.getTimeSec()}%.3f sec")
          }
        }


        logInfo(f"Time taken to deserialize all pipelined blocks: ${sw2.getTimeSec()}%.3f sec")


        KryoUtils.Pool.release(kryo)
        finalRowsList
      }

      persistenceModel match {
        case SqlOptionModelHash => readRows()
        case SqlOptionModelBinary => readRows()
        case SqlOptionModelBlock => readBlocks()
      }
    }
  }


}

object RedisSourceRelation {

  val RowClass: Class[Row] = classOf[Row]
  val GenericRowClass: Class[GenericRow] = classOf[GenericRow]

  def schemaKey(tableName: String): String = s"_spark:$tableName:schema"

  def dataKey(tableName: String, partitionId: Integer, id: String = uuid()): String = {
    s"$tableName:{$partitionId}:$id"
  }

  def uuid(): String = UUID.randomUUID().toString.replace("-", "")

  def tableDataKeyPattern(tableName: String): String = s"$tableName:*"

  def tableKey(keysPrefixPattern: String, redisKey: String): String = {
    if (keysPrefixPattern.endsWith("*")) {
      // keysPattern*
      redisKey.substring(keysPrefixPattern.length - 1)
    } else {
      redisKey
    }
  }
}
