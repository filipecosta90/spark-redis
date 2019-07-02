package org.apache.spark.sql.redis

import java.util.UUID

import com.esotericsoftware.kryo.io.{Input, Output}
import com.redislabs.provider.redis.rdd.Keys._
import com.redislabs.provider.redis.util.ConnectionUtils.withConnection
import com.redislabs.provider.redis.util.PipelineUtils._
import com.redislabs.provider.redis.util.{KryoUtils, Logging, StopWatch, StopWatchAdv}
import com.redislabs.provider.redis.{ReadWriteConfig, RedisConfig, RedisDataTypeHash, RedisDataTypeString, RedisEndpoint, RedisNode, toRedisContext}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
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
  private val kryoBufferSize = parameters.get(SqlOptionRedisKryoSerializerBufferSizeMB).map(_.toInt).getOrElse(SqlOptionRedisKryoSerializerBufferSizeMBDefault) * 1024 * 1024
  logInfo(f"Using kryoBufferSize of: ${kryoBufferSize} bytes")


  /**
    * redis key pattern for rows, based either on the 'keys.pattern' or 'table' parameter
    */
  private val dataKeyPattern = keysPatternOpt
    .orElse(tableNameOpt.map(tableName => tableDataKeyPattern(tableName)))
    .getOrElse {
      val msg = s"Neither '$SqlOptionKeysPattern' or '$SqlOptionTableName' option is set."
      throw new IllegalArgumentException(msg)
    }

  logInfo(f"Using persistence model: ${persistenceModel}")
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
    kryo.addDefaultSerializer(RowClass, new RowSerializer(schema))
    kryo.register(RowClass)
    kryo.register(GenericRowClass)
    val partId = TaskContext.getPartitionId()

    partition.grouped(blockSize).foreach { rows =>
      val stopWatch = new StopWatch()
      var marker = 0.0
      var output = new Output(kryoBufferSize)
      output.setPosition(0)
      //save the number of row in that block

      output.writeVarInt(rows.length, true)
      logInfo(f"writting ${rows.length} Rows in key")
      rows.foreach { row =>
        kryo.writeObject(output, row)
      }
      output.flush()
      val serializedBlock = output.toBytes
      output.close()
      if (logInfoVerbose) {
        logInfo(f"writeBlocks step(3/4) :: Time taken to serialize block with $blockSize rows (${serializedBlock.size} bytes) in partition $partId: ${stopWatch.getTimeSec()}%.3f sec")
        marker = stopWatch.getTimeSec()
      }
      // TODO: pipeline?
      val blockKey = dataKey(tableName())
      val conn = redisConfig.connectionForKey(blockKey)
      conn.set(blockKey.getBytes, serializedBlock)
      conn.close()
      if (logInfoVerbose) {
        logInfo(f"writeBlocks step(4/4) :: Time taken to SET block with $blockSize rows in partition $partId: ${stopWatch.getTimeSec() - marker}%.3f sec")
        logInfo(f"writeBlocks allSteps :: All steps time with block with $blockSize rows in partition $partId: ${stopWatch.getTimeSec()}%.3f sec")
      }
    }
    KryoUtils.Pool.release(kryo)

    if (logInfoVerbose) {
      logInfo(f"writeBlocks ALL blocks: ${stopWatchWriteBlocks.getTimeSec()}%.3f sec")
    }

  }

  private def writeRows(partition: Iterator[Row]): Unit = {
    // grouped iterator to only allocate memory for a portion of rows
    partition.grouped(iteratorGroupingSize).foreach { batch =>
      val stopWatch = new StopWatch()
      // the following can be optimized to not create a map
      val rowsWithKey: Map[String, Row] = batch.map(row => dataKeyId(row) -> row).toMap
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
        val partId = TaskContext.getPartitionId()
        logInfo(f"Time taken to write ${batch.size} rows in partition $partId: ${stopWatch.getTimeSec()}%.3f sec")
      }
    }
  }

  /**
    * @return redis key for the row
    */
  private def dataKeyId(row: Row): String = {
    val id = keyColumn.map(id => row.getAs[Any](id)).map(_.toString).getOrElse(uuid())
    dataKey(tableName(), id)
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
    val keysRdd = sc.fromRedisKeyPattern(dataKeyPattern, partitionNum = numPartitions)
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
        // grouped iterator to only allocate memory for a portion of rows
        partition.grouped(iteratorGroupingSize).flatMap { batch =>
          val stopWatch = new StopWatch()
          val rows = groupKeysByNode(redisConfig.hosts, batch)
            .flatMap { case (node, keys) =>
              scanRows(node, keys, keyType, requiredSchema, requiredColumns)
            }
          if (logInfoVerbose) {
            val partId = TaskContext.getPartitionId()
            logInfo(f"Time taken to read ${batch.size} values/blocks in partition $partId: ${stopWatch.getTimeSec()}%.3f sec")
          }
          rows
        }
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
        kryo.addDefaultSerializer(RowClass, new RowSerializer(schema))
        kryo.register(RowClass)
        kryo.register(GenericRowClass)

        // TODO:
        def project(fullRow: Row, requiredSchema: StructType): Row = {
          val cols: Array[Any] = fullRow.getValuesMap(requiredSchema.fieldNames).values.toArray
          new GenericRow(cols)
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
            var numRows = input.readInt(true)
            logInfo(f"reading $numRows rows from redis with available bytes ${input.available}")
            for (rowNumber <- 0 until numRows) {
              val deserializedRow = kryo.readObject(input, RowClass)
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

        //        val sw1 = new StopWatch()
        //        filteredKeys.foreach( key => {
        //         val bytes: Array[Byte] = conn.get(key.getBytes)
        ////          logInfo(f"Key ${key} has ${bytes.length} bytes")
        //          if ( bytes.length > 0 ){
        //            val input = new Input(bytes)
        //            input.setPosition(0)
        //            var numRows = input.readInt(true)
        ////            logInfo(f"reading ${numRows} rows from redis key ${key} with available bytes ${input.available}")
        //            for ( rowNumber <- 0 to numRows - 1){
        //              val row = kryo.readObject(input, classOf[Row])
        //              finalRowsList :+ row
        //            }
        //            input.close()
        //          }
        //
        //        })
        KryoUtils.Pool.release(kryo)
        finalRowsList
      }


      //
      //
      //        val res = pipelineValues.map { v =>
      //          val serializedBlock = v.asInstanceOf[Array[Byte]]
      //          v.asInstanceOf[Array[Byte]].map(bytes => {
      //            val input = new Input(bytes)
      //            project( kryo.readObject(input, classOf[Row] ), persistedSchema, requiredSchema )
      //          })
      //        }
      //        logInfo(f"Time taken to deserialize blocks to objects (Kryo time): ${swKryo.getTimeSec()}%.3f sec")
      //        logInfo(f"Time taken to deserialize and convert to Row: ${sw2.getTimeSec()}%.3f sec")
      //        KryoUtils.Pool.release(kryo)
      //        res
      //      }

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

  def dataKey(tableName: String, id: String = uuid()): String = {
    s"$tableName:$id"
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
