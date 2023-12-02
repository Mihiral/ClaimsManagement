package com.ClaimsProcess

import org.apache.spark.sql.functions.{broadcast, col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeltaLakeWriter(spark: SparkSession) {
  def writeIncremental(dataFrame: DataFrame): Unit = {
    // Remove null values from the DataFrame
    val dfWithoutNulls = dataFrame.na.drop()
    val dfWithoutDuplicates = dfWithoutNulls.dropDuplicates()
    //Read data from pharmacy database as a dataframe and use it as a look up
    val jdbcUrl = "jdbc:mysql://JDBC78:2324/PHRXT"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val query = "(SELECT * FROM pharmacy_all where start_date = sysdate)"
    val pharmaDF = spark.read.jdbc(url = jdbcUrl, table = query, properties = connectionProperties)
    val joinedDF = dfWithoutDuplicates.join(broadcast(pharmaDF), "id")


    // Define UDFs for calculating claim amounts based on category and medicine type
    val calculateClaimAmountUDF = udf((category: String, medicineType: String) => {
      // Define rules for claim amounts based on category and medicine type
      val baseClaimAmount = 1000 // Base claim amount for all categories

      // Rules for different patient categories and medicine types
      val claimAmount = category match {
        case "A" =>
          medicineType match {
            case "Type1" => baseClaimAmount + 500 // Category A, Type1 medicine
            case "Type2" => baseClaimAmount + 700 // Category A, Type2 medicine
            case _ => baseClaimAmount // Other medicine types in Category A
          }
        case "B" =>
          medicineType match {
            case "Type1" => baseClaimAmount + 300 // Category B, Type1 medicine
            case "Type2" => baseClaimAmount + 600 // Category B, Type2 medicine
            case _ => baseClaimAmount // Other medicine types in Category B
          }
        case _ => baseClaimAmount // Default claim amount for other categories
      }

      claimAmount
    })

    // Apply the custom transformation using the UDF
    val resultDF = joinedDF
      .withColumn("calculatedClaimAmount", calculateClaimAmountUDF(col("category"), col("medicineType")))

    // Show the result
    resultDF.show()

    joinedDF.write.format("delta").mode("append").save("/mnt/databricks-delta-table")
  }
}
