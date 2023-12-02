package com.ClaimsProcess

import org.apache.spark.sql.SparkSession
import com.ClaimsProcess._

object InsuranceClaimProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("InsuranceClaimProcessor")
      .getOrCreate()

    val kafkaTopic = "insurance-claims-topic"
    val jsonDataTransformer = new JsonDataTransformer(spark)
    val deltaLakeWriter = new DeltaLakeWriter(spark)
    // Fetch JSON data from Kafka and transform it
    val jsonStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka_broker1:9092,kafka_broker2:9092")
      .option("subscribe", kafkaTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")

    val transformedData = jsonDataTransformer.transform(jsonStream)
    // Write transformed data to Delta Lake in an incremental way
    deltaLakeWriter.writeIncremental(transformedData)
    spark.streams.awaitAnyTermination()
  }}

