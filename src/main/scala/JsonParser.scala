import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object JsonParser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("TabellaQuery")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val input = "D:\\BigData\\ProgettoBigData\\Firs500Rows.json"

  import hiveContext.implicits._
  def main(args: Array[String]){

    //parso il file e lo visualizzo
    val df = sqlContext.read.json(input)
    val new_df = df.withColumnRenamed("public", "publicField")
    //creo dataSet
    val ds_Event = new_df.as[Event]
    //creo rdd
    val rdd_event = ds_Event.rdd
    rdd_event.take(10).foreach(println)

    /*per vedere la stampa corretta
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
    data4.show()*/

    /*
    //conto numero event per ogni actor
    val pairRdd = eventRdd.map(x => (x.actor, 1L))
    val actorEvent = pairRdd.reduceByKey(_+_)
    eventRdd.foreach(println)*/

  }
}