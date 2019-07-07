package org.apache.spark.sql.redis

import java.util.UUID

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
import redis.clients.jedis.{PipelineBase, Protocol}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

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

    // write data
    data.foreachPartition { partition =>
      persistenceModel match {
        case SqlOptionModelHash => writeRows(partition)
        case SqlOptionModelBinary => writeRows(partition)
        case SqlOptionModelBlock => {
          writeBlocks(partition, schema)
        }
      }
    }

    logInfo(f"Time taken to insert data: ${stopWatch.getTimeSec()}%.3f sec")

  }

  private def writeBlocks(partition: Iterator[Row], schema: StructType): Unit = {
    val stopWatchWriteBlocks = new StopWatch()
    val kryo = KryoUtils.Pool.borrow()
    val rowSerializer = new RowSerializer(schema)
    kryo.addDefaultSerializer(RowClass, rowSerializer)
    kryo.register(RowClass)
    kryo.register(GenericRowClass)
    val partId = TaskContext.getPartitionId()

    val table = tableName()
    var currentRedisKey = dataKey(table, partId)

    // assuming that the partition keys belong all to the same redis instance via :{partition_id}:
    val conn = redisConfig.connectionForKey(currentRedisKey)
    val buffer: Array[Byte] = new Array[Byte](kryoBufferSize)

    var pipeline = conn.pipelined()
    var pipePosition : Integer = 0
    var output = new Output(buffer)

    var numberRowsInBlock  : Integer = 0
    var blockNum  : Integer = 0
    var timeBlockStart: Long = 0
    var timeBlockEnd: Long = 0
    var timeRowSerialize: Long = 0
    var timeRowPipeline : Long= 0
    var timeRowPipelineSync: Long = 0

    logInfo(f"writeBlocks setup time forPartion ID: ${partId}  ${stopWatchWriteBlocks.getTimeSec()}%.3f sec")
    logInfo(f"Partion ID: ${partId} Using output buffer length of ${buffer.length} Bytes")

    val stopWatch = new StopWatch()
    var marker: Long  = stopWatch.getTime()

    for (row <- partition) {
      marker = stopWatch.getTime()
      numberRowsInBlock = numberRowsInBlock + 1

      // runs once per block ( at the beginning )
      //if first row in block
      //save the number of rows in that block
      if (numberRowsInBlock == 1) {
        output.setPosition(0)
        output.writeInt(1)
        pipeline.append(currentRedisKey.getBytes, buffer.slice(0, output.position()))
        timeBlockStart = timeBlockStart + (stopWatch.getTime()-marker)
        marker=stopWatch.getTime()

      }
      output.setPosition(0)
      rowSerializer.write(kryo, output, row)
      // ROW Serializer 1.1
      timeRowSerialize = timeRowSerialize + (stopWatch.getTime()-marker)
      marker=stopWatch.getTime()

      pipeline.append(currentRedisKey.getBytes, buffer.slice(0, output.position()))
      pipePosition = pipePosition + 1
      // ROW REDIS PIPELINE.APPEND 1.2
      timeRowPipeline = timeRowPipeline + (stopWatch.getTime()-marker)
      marker=stopWatch.getTime()




      // runs once per block ( at the end )
      if (numberRowsInBlock == blockSize) {
        //now that we got to the block limit lets save its correct size
        output.setPosition(0)
        output.writeInt(numberRowsInBlock)
        pipeline.setrange(currentRedisKey.getBytes, 0, buffer.slice(0, output.position()))
        //swap to block and redis new key
        blockNum += 1
        numberRowsInBlock = 0
        currentRedisKey = dataKey(table, partId)
        // BLOCK END 2.0
        timeBlockEnd = timeBlockEnd + (stopWatch.getTime()-marker)
        marker=stopWatch.getTime()

      }

      if (pipePosition >= readWriteConfig.maxPipelineSize) {
        pipeline.sync()
        pipeline = conn.pipelined()
        pipePosition = 0
        // ROW REDIS PIPELINE.SYNC 1.3
        timeRowPipelineSync = timeRowPipelineSync + (stopWatch.getTime()-marker)
        marker=stopWatch.getTime()
      }

    }


    //save the size of last block if it has more than one row
    if (numberRowsInBlock > 1) {
      output.setPosition(0)
      output.writeInt(numberRowsInBlock)
      pipeline.setrange(currentRedisKey.getBytes, 0, buffer.slice(0, output.position()))
    }
    KryoUtils.Pool.release(kryo)

    // sync remaining items
    if (pipePosition % readWriteConfig.maxPipelineSize != 0) {
      marker = stopWatch.getTime()
      pipeline.sync()
      pipePosition = 1
      logInfo(f"writeBlocks REDIS PIPELINE.SYNC FINAL :: Final SYNC PIELINE with $pipePosition OPPS in partition $partId: ${stopWatch.getTimeSec()-marker}%.3f sec")
    }



    if (logInfoVerbose) {
      logInfo(f"BLOCK START 0.1 ${timeBlockStart} msec")
      logInfo(f"ROW REDIS SERIALIZE 1.1 ${timeRowSerialize} msec")
      logInfo(f"ROW REDIS PIPELINE.APPEND 1.1 ${timeRowPipeline} msec")
      logInfo(f"BLOCK END 2.0 ${timeBlockEnd} msec")
      logInfo(f"ROW REDIS PIPELINE.SYNC 1.3 ${timeRowPipelineSync} msec")
    }

    conn.close()

    if (logInfoVerbose) {
      logInfo(f"writeBlocks ALL blocks: ${stopWatchWriteBlocks.getTimeSec()}%.3f sec")
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
            logInfo(f"Time taken to read ${batch.size} values/blocks in partition $partId: ${stopWatch.getTimeSec()-marker}%.3f sec")
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


        // TODO:
        def project(fullRow: Row, requiredSchema: StructType): Row = {
          val cols = requiredSchema.fieldNames.map(fieldName => fullRow.getAs[Any](fieldName))
          new GenericRowWithSchema(cols,schema)
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
            var numRows : Integer = input.readInt()
            logInfo(f"reading $numRows rows from redis with available bytes ${input.available}")
            for (rowNumber <- 0 until numRows) {
//              logInfo(f"reading row $rowNumber")
              val deserializedRow = rowSerializer.read(kryo,input,RowClass)
              //val deserializedRow = kryo.readObject(input, RowClass)
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
