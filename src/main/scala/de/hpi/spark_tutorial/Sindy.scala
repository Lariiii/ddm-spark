package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    val tables = inputs.map { source =>
      spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(source)
    }
    tables.foreach(_.printSchema())
    tables.foreach(_.show())

  }
}
