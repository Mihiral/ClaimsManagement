package com.ClaimsProcess

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonDataTransformer(spark: SparkSession) {
  def transform(jsonStream: DataFrame): DataFrame = {
    val flatteneddf = flattenDataframe(jsonStream)
    def flattenDataframe(df: DataFrame): DataFrame = {
      //getting all the fields from schema
      val fields = df.schema.fields
      val fieldNames = fields.map(x => x.name)
      //length shows the number of fields inside dataframe
      val length = fields.length
      for (i <- 0 to fields.length - 1) {
        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name
        fieldtype match {
          case arrayType: ArrayType =>
            val fieldName1 = fieldName
            val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
            //val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName1.*"))
            val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
            return flattenDataframe(explodedDf)

          case structType: StructType =>
            val childFieldnames = structType.fieldNames
              .map(childname => fieldName + "." + childname)
            val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").
              replace("$", "_").replace("__", "_").replace(" ", "").
              replace("-", ""))))
            val explodedf = df.select(renamedcols: _*)
            return flattenDataframe(explodedf)
          case _ =>
        }
      }
      df
    }
    flatteneddf
  }
}

