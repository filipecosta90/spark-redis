package org.apache.spark.sql.redis

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class RowSerializer(var s: StructType) extends Serializer[Row] {

  var schema = s
  var dataTypes = new Array[DataType](0)
  if (s != null) {
    dataTypes = new Array[DataType](schema.size)
    var pos = 0
    schema.foreach { field: StructField => {
      dataTypes(pos) = field.dataType
      pos += 1
    }
    }
  }

  // TODO: assess with Oleksiy (@fe2s) if the datatTypes are all covered
  override def write(kryo: Kryo, output: Output, t: Row): Unit = {

    // write the number of fields
    output.writeInt(t.length)

    for (i <- 0 to t.length - 1) {

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

  override def read(kryo: Kryo, input: Input, aClass: Class[Row]): Row = {

    // read the number of fields
    val size = input.readInt()
    val cols = new Array[Any](size)

    for (fieldnum <- 0 to (size - 1 ) ) {

      dataTypes(fieldnum) match {
        case StringType => cols(fieldnum) = input.readString()
        case BooleanType => cols(fieldnum) = input.readBoolean()
        case ByteType => cols(fieldnum) = input.readByte()
        case ShortType => cols(fieldnum) = input.readShort()
        case IntegerType => cols(fieldnum) = input.readInt()
        case LongType => cols(fieldnum) = input.readLong()
        case FloatType => cols(fieldnum) = input.readFloat()
        case DoubleType => cols(fieldnum) = input.readDouble()
        case _ => cols(fieldnum) = kryo.readClassAndObject(input)
      }
    }


    var newRow: Row = new GenericRowWithSchema(cols, schema)
    newRow
  }
}