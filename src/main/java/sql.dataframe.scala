import org.apache.spark.sql.types.{FloatType, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.TraversableLike

/**
  * Created by lws4 on 30.06.2016.
  */
class SqlDataFrames(sqlContext: SQLContext, input:String) {

  /*sql scheme of GDELT dataset*/

//InvoiceNo	StockCode	Description	Quantity	InvoiceDate	UnitPrice	CustomerID	Country

  var gdeltSchema = StructType(Array(
    StructField("InvoiceNo", StringType, true),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", StringType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", IntegerType, true),
    StructField("Country", StringType, true)))

  var antecSchemaDef = StructType(Array(
    StructField("RuleID", LongType, true),
    StructField("AntecStockCode", StringType, true)
  ))

  /*  Initialize SQLContext of GDELT  */
  var sqlContextData = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .schema(this.gdeltSchema)
    .load(input)



  def readPurchases(): DataFrame = {
    val result = sqlContextData.select("InvoiceNo", "StockCode")
    return result
  }

  def readPurchasesWithDescription(): DataFrame = {
    val result = sqlContextData.select("StockCode", "Description").distinct()
    return result
  }

  def readAntecSchema(rdd: RDD[Row]): DataFrame = {
    val result = sqlContext.createDataFrame(rdd, antecSchemaDef)
    return result
  }

}
