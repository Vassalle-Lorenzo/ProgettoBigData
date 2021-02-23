import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime



object JsonParser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("TabellaQuery")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val input = "D:\\BigData\\ProgettoBigData\\2018-03-01-0.json"

  import hiveContext.implicits._
  def main(args: Array[String]) {
    //data frame
    val df = sqlContext.read.json(input)
    df.registerTempTable("miaTabella")

    val new_df = df.withColumnRenamed("public", "publicField")
    new_df.show()

    //stampo l'intera tabella
    val data = sqlContext.sql("select * from miaTabella")
    data.show()
    //trovo tutti gli actor
    val data2 = sqlContext.sql("select actor from miaTabella")
    data2.show()
    //trovo tutti i repo
    val data3 = sqlContext.sql("select repo from miaTabella")
    data3.show()
    //contare gli actor
    val data4 = sqlContext.sql("select count(actor) from miaTabella")
    data4.show()
    //rdd
    val eventRdd = df.as[Event].rdd
    sc.stop()
  }
}