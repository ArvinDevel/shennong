package org.apache.spark.sql.cstream

import java.io.Closeable

import org.apache.bookkeeper.clients.impl.stream.event.StreamPositionImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

class MetadataReader(options: CStreamOptions) extends Closeable with Logging {

  import TPCH_TABLE_NAMES._
  import me.jinsui.shennong.bench.avro._

  def getSchema(): StructType = {
    getSchema(options.tbl)
  }

  // TODO: read schema from stream/schema registry later
  def getSchema(tbl: String): StructType = {
    val avroSchema = tbl match {
      case LINEITEM => Lineitem.getClassSchema
      case ORDERS => Orders.getClassSchema
      case PART => Part.getClassSchema
      case PARTSUPP => Partsupp.getClassSchema
      case NATION => Nation.getClassSchema
      case SUPPLIER => Supplier.getClassSchema
      case REGION => Region.getClassSchema
      case CUSTOMER => Customer.getClassSchema
      case _ => throw new NotImplementedError(s"not supported table: $tbl")
    }

    val dt = SchemaConverters.toSqlType(avroSchema).dataType
    assert(dt.isInstanceOf[StructType])
    dt.asInstanceOf[StructType]
  }

  def getStartPos(): StreamPositionImpl = {

  }

  def getEndPos(): StreamPositionImpl = {

  }

  override def close(): Unit = {}
}
