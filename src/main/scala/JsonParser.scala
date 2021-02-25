import org.apache.spark.sql.functions.{col, explode}
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

    //parso il file
    val df_event = sqlContext.read.json(input)
    val new_df_event = df_event.withColumnRenamed("public", "publicField")
    //creo dataSet
    val ds_event = new_df_event.as[Event]
    //creo rdd
    val rdd_event = ds_event.rdd


    //TODO:1.1)trovare i singoli actor
    //DF
    val df_actor = new_df_event.select("actor").distinct()
    df_actor.show()
    //RDD
    val rdd_actor = rdd_event.map(x => x.actor).distinct()
    rdd_actor.take(10).foreach(println)

    //TODO:1.2)trovare i singoli author dentro commit
    ////DF = cambio da event a commit
    val payload_df = df_event.select("payload.*")
    val commits_df = payload_df.select(explode(col("commits"))).select("col.*")
    val author_df = commits_df.select("author")
    author_df.show()
    //RDD
    val rdd_commit = commits_df.as[Commit].rdd
    val rdd_author = rdd_commit.map(x => x.author).distinct()
    rdd_author.take(10).foreach(println)

    //TODO:1.3)trovare i singoli repo
    //DF
    val df_repo = new_df_event.select("repo").distinct()
    df_repo.show()
    //RDD
    val rdd_repo = rdd_event.map(x => x.repo).distinct()
    rdd_repo.take(10).foreach(println)

    //TODO:1.4)trovare i vari tipi di evento type
    //DF
    val df_type = new_df_event.select("`type`").distinct()
    df_type.show()
    //RDD
    val rdd_type = rdd_event.map(x => x.`type`).distinct()
    rdd_type.take(10).foreach(println)

    //TODO:1.5)contare il numero di actor
    //DF
    val df_acto = new_df_event.select("actor").distinct().count()
    println(df_acto)
    //RDD
    val rdd_ac = rdd_event.map(x => x.actor).distinct().count()
    println(rdd_ac)

    //TODO:1.6)contare il numero di repo
    //DF
    val df_rep = new_df_event.select("repo").distinct().count()
    println(df_rep)
    //RDD
    val rdd_rep = rdd_event.map(x => x.repo).distinct().count()
    println(rdd_rep)

    //TODO:2.1)contare numero event per ogni actor
    //DF
    val df_nEvent = new_df_event.select("actor").count()
    println(df_nEvent)
    //RDD
    val rdd_a = rdd_event.map(x => x.actor).count()
    println(rdd_a)

    //TODO: 2.2)contare il numero di event divisi



    //TODO:esempio prof
    /*//nPagine per ogni autore
    val paginePerAutore = libriRDD.map(x => (x.autore, x.pagine))
    val reduce = paginePerAutore.reduceByKey((p1,p2) => p1+p2)*/

    /*per vedere la stampa corretta
    //stampo l'intera tabella
    val data = sqlContext.sql("select * from miaTabella")
    data.show()*/
  }
}