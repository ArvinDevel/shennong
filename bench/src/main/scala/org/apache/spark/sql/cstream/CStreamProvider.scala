package org.apache.spark.sql.cstream

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.util.Utils

class CStreamProvider extends DataSourceRegister with RelationProvider with Logging {
  override def shortName(): String = "cstream"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {

    val options = new CStreamOptions(parameters)
    val schema = Utils.tryWithResource(new MetadataReader(options)) { reader =>
      reader.getSchema()
    }

    new CStreamRelation(
      sqlContext,
      schema,
      options)
  }
}
