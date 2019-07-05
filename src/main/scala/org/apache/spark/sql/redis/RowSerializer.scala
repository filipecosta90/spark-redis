package org.apache.spark.sql.redis

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

class RowSerializer(val schema: StructType) extends Serializer[Row] {

  private val dataTypes: Array[DataType] = schema.fields.map(_.dataType)
  private val nullableRows: Array[Boolean] = schema.fields.map(_.nullable)
  private val LOG: Logger = LoggerFactory.getLogger(classOf[RowSerializer])

  // TODO: assess with Oleksiy (@fe2s) if the datatTypes are all covered
  override def write(kryo: Kryo, output: Output, t: Row): Unit = {


    // write the number of fields
    output.writeInt(t.length)

    for (i <- 0 until t.length) {
      //if the column is allowed to be null
      if (nullableRows(i)) {
        output.writeByte(if (t.isNullAt(i)) 1 else 0)
      }
      if (!t.isNullAt(i)) {
        dataTypes(i) match {
          case StringType => output.writeString(t.getAs[String](i))
          case BooleanType => output.writeBoolean(t.getAs[Boolean](i))
          case ByteType => output.writeByte(t.getAs[Byte](i))
          case ShortType => output.writeShort(t.getAs[Short](i))
          case IntegerType => output.writeInt(t.getAs[Int](i))
          case LongType => output.writeLong(t.getAs[Long](i))
          case FloatType => output.writeFloat(t.getAs[Float](i))
          case DoubleType => output.writeDouble(t.getAs[Double](i))
          case _ => kryo.writeClassAndObject(output, t.get(i))
        }
      }


    }


  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Row]): Row = {

    // read the number of fields
    val size = input.readInt()
    val cols = new Array[Any](size)
    //LOG.info(s"#COLS ${size}")

    for (fieldnum <- 0 until size) {
      var isNull: Byte = 0
      if (nullableRows(fieldnum)) {
        isNull = input.readByte()
      }
      var fieldVal: Any = null
      if (isNull == 0) {
        fieldVal = dataTypes(fieldnum) match {
          case StringType => input.readString()
          case BooleanType => input.readBoolean()
          case ByteType => input.readByte()
          case ShortType => input.readShort()
          case IntegerType => input.readInt()
          case LongType => input.readLong()
          case FloatType => input.readFloat()
          case DoubleType => input.readDouble()
          case _ => kryo.readClassAndObject(input)
        }
      }
      cols(fieldnum) = fieldVal
    }

    new GenericRowWithSchema(cols, schema)
  }
}