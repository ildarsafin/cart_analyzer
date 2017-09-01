import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{FloatType, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object ShopingListFrequentItems {

  /*Init Spark Context*/
  def initSparkContext():Tuple2[SparkContext,SQLContext] =
  {
    /*  Initialize Spark Context*/
    val conf: SparkConf = new SparkConf()
      .setAppName("ShoppingListFrequentItems")
      .set("spark.executor.memory", "100g")
      .set("spark.driver.memory", "100g")
      .set("spark.driver.maxResultSize","100g")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    return (sc,sqlContext)
  }

  def main(args: Array[String]) {

    /* input file */
    val input_file = "C:\\Spark\\Online Retail.csv"

    /*Init Spark Context*/
    val (sc, sqlContext) = initSparkContext()

    val SqlDataFrame = new SqlDataFrames(sqlContext,input_file)

    /* Extract Data*/
    val purchases = SqlDataFrame.readPurchases()
    val refactorRename = purchases.map(v => (v.getString(0), v.getString(1))).groupByKey().map(t=>t._2.toArray.distinct)

    val fpg = new FPGrowth().setMinSupport(0.01).setNumPartitions(100)

    val model = fpg.run(refactorRename)

    val minConfidence = 0.8

    val rules = model.generateAssociationRules(minConfidence).zipWithUniqueId()

    val antecedentRDD =  rules.flatMap(r => r._1.antecedent.map(t => Row(r._2, t)))
    val consequentRDD =  rules.flatMap(r => r._1.consequent.map(t => Row(r._2, t)))

    val product = SqlDataFrame.readPurchasesWithDescription()
    var contAnt = SqlDataFrame.readAntecSchema(antecedentRDD)
    var contCons = SqlDataFrame.readAntecSchema(consequentRDD)

    var joinResultAnt = contAnt.join(product, contAnt.col("AntecStockCode") === product.col("StockCode"), "left").select("RuleID", "StockCode", "Description").distinct()
    var joinResultCons = contCons.join(product, contCons.col("AntecStockCode") === product.col("StockCode"), "left").select("RuleID", "StockCode", "Description").distinct()

    joinResultAnt.show()
    joinResultCons.show()

    var totalResult = joinResultAnt.map(v => (v(0), v(2))).groupByKey().map(t=>(t._1,t._2.toArray.mkString(", ")))
    var totalResultCons = joinResultCons.map(v => (v(0), v(2))).groupByKey().map(t=>(t._1,t._2.toArray.mkString(", ")))

    var result = totalResult.cogroup(totalResultCons).map(t => "Rule:" + t._1+" [" + t._2._1.toArray.mkString(", ") + "] = > " + t._2._2.toArray.mkString(", "))
    result.coalesce(1).saveAsTextFile("C:\\Spark\\result.txt")
    result.foreach(println)
  }

}


