package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO
    println(inputs.head)

    val region = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(inputs.head)

    region.printSchema()
    region.show()
  }
}
