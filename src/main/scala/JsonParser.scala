import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, explode, max, min, second}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import java.util.Date
import scala.math.Ordering.{Tuple2, comparatorToOrdering}
import scala.tools.scalap.scalax.rules.scalasig.ScalaSigEntryParsers.entryType


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
    //DF = cambio da event a commit
    val payload_df = df_event.select("payload.*")
    val commits_df = payload_df.select(explode(col("commits"))).select("col.*")
    val author_df = commits_df.select("author").distinct()
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

    //TODO: 2.2)contare il numero di event divisi per type e actor
    //DF
    val df_ev = new_df_event.select(($"type"), ($"actor"), count($"*").over(Window.partitionBy("type", "actor")) as "nEvent")
    df_ev.show()
    //RDD
    val rdd_e = rdd_event.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((e1,e2) => e1+e2)
    rdd_e.take(10).foreach(println)

    //TODO: 2.3)contare il numero di event divisi per type, actor e repo
    //DF
    val df_eve = new_df_event.select($"type", $"actor", $"repo", count($"*").over(Window.partitionBy($"type", $"actor", $"repo")) as "nEvent")
    df_eve.show()
    //RDD
    val rdd_ev = rdd_event.map(x => ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((e1,e2) => e1+e2)
    rdd_ev.take(10).foreach(println)

    //TODO: 2.4)contare gli event divisi per type, actor, repo e secondo trasformare timestamp per avere solo il secondo valore, raggruppa su quest'ultimo
    //DF
    val df_date = new_df_event.withColumn("second", second($"created_at"))
    val df_even = df_date.select($"type", $"actor", $"repo", $"second", count($"*").over(Window.partitionBy($"type", $"actor", $"repo", $"second")) as "nEvent")
    df_even.show()
    //RDD
    val rdd_even = rdd_event.map(x=> ((x.`type`, x.actor, x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    rdd_even.take(10).foreach(println)

    //TODO: 2.5)trova max e min numero di event per secondo
    //DF_max
    val df_date_max = new_df_event.withColumn("second", second($"created_at"))
    val df_ev_max = df_date_max.select($"second", count($"*").over(Window.partitionBy($"second")) as "conteggio")
    val df_max_ev = df_ev_max.agg(max("conteggio"))
    df_max_ev.show()
    //RDD_max
    val rdd_max_date = rdd_event.map(x=> (new DateTime(x.created_at.getTime).getSecondOfMinute, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_da = rdd_max_date.map(x => x._2).max()
    println(rdd_max_da)
    //DF_min
    val df_date_min = new_df_event.withColumn("second", second($"created_at"))
    val df_ev_min = df_date_min.select($"second", count($"*").over(Window.partitionBy($"second")) as "conteggio")
    val df_min_ev = df_ev_min.agg(min("conteggio"))
    df_min_ev.show()
    //RDD_min
    val rdd_min_date = rdd_event.map(x=> (new DateTime(x.created_at.getTime).getSecondOfMinute, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_da = rdd_min_date.map(x => x._2).min()
    println(rdd_min_da)

    //TODO: 2.6)trova max e min numero di event per actor
    //DF_max
    val data_frame_max_actor = new_df_event.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "conteggio")
    val df_max_actor = data_frame_max_actor.agg(max("conteggio"))
    df_max_actor.show()
    //RDD_max
    val rdd_max_actor = rdd_event.map(x => (x.actor.id, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_a = rdd_max_actor.map(x => x._2).max()
    println(rdd_max_a)
    //DF_min
    val data_frame_min_actor = new_df_event.select($"actor", count($"*").over(Window.partitionBy($"actor")) as "conteggio")
    val df_min_actor = data_frame_min_actor.agg(min("conteggio"))
    df_min_actor.show()
    //RDD_min
    val rdd_min_actor = rdd_event.map(x => (x.actor.id, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_a = rdd_min_actor.map(x => x._2).min()
    println(rdd_min_a)

    //TODO: 2.7)trova max e min numero di event per repo
    //DF_max
    val data_frame_max_repo = new_df_event.select($"repo", count($"*").over(Window.partitionBy($"repo")) as "conteggio")
    val df_max_repo = data_frame_max_repo.agg(max("conteggio"))
    df_max_repo.show()
    //RDD_max
    val rdd_max_repo = rdd_event.map(x => (x.repo, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_r = rdd_max_repo.map(x => x._2).max()
    println(rdd_max_r)
    //DF_min
    val data_frame_min_repo = new_df_event.select($"repo", count($"*").over(Window.partitionBy($"repo")) as "conteggio")
    val df_min_repo = data_frame_min_repo.agg(min("conteggio"))
    df_min_repo.show()
    //RDD_min
    val rdd_min_repo = rdd_event.map(x => (x.actor.id, x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_r = rdd_min_repo.map(x => x._2).min()
    println(rdd_min_r)

    //TODO: 2.8)trova max e min numero di event per secondo per actor
    //DF_max
    val df_mx_aS = new_df_event.withColumn( "second", second($"created_at"))
    val data_frame_max_actorS = df_mx_aS.select( $"second", $"actor", count($"*").over(Window.partitionBy( $"second", $"actor")) as "conteggio")
    val df_max_actorS = data_frame_max_actorS.agg(max("conteggio"))
    df_max_actorS.show()
    //RDD_max
    val rdd_max_actorS = rdd_event.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_aS = rdd_max_actorS.map(x => x._2).max()
    println(rdd_max_aS)
    //DF_min
    val df_mn_aS = new_df_event.withColumn( "second", second($"created_at"))
    val data_frame_min_actorS = df_mn_aS.select( $"second", $"actor", count($"*").over(Window.partitionBy( $"second", $"actor")) as "conteggio")
    val df_min_actorS = data_frame_min_actorS.agg(min("conteggio"))
    df_min_actorS.show()
    //RDD_min
    val rdd_min_actorS = rdd_event.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_aS = rdd_min_actorS.map(x => x._2).min()
    println(rdd_min_aS)

    //TODO: 2.9)trova max e min numero di event per secondo per repo
    //DF_max
    val df_mx_rS = new_df_event.withColumn( "second", second($"created_at"))
    val data_frame_max_repoS = df_mx_rS.select( $"second", $"repo", count($"*").over(Window.partitionBy( $"second", $"repo")) as "conteggio")
    val df_max_repoS = data_frame_max_repoS.agg(max("conteggio"))
    df_max_repoS.show()
    //RDD_max
    val rdd_max_repoS = rdd_event.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_rS = rdd_max_repoS.map(x => x._2).max()
    println(rdd_max_rS)
    //DF_min
    val df_mn_rS = new_df_event.withColumn( "second", second($"created_at"))
    val data_frame_min_repoS = df_mn_rS.select( $"second", $"repo", count($"*").over(Window.partitionBy( $"second", $"repo")) as "conteggio")
    val df_min_repoS = data_frame_min_repoS.agg(min("conteggio"))
    df_min_repoS.show()
    //RDD_min
    val rdd_min_repoS = rdd_event.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_rS = rdd_min_repoS.map(x => x._2).min()
    println(rdd_min_rS)

    //TODO: 2.10)trova max e min numero di event per secondo per repo e actor
    //DF_max
    val df_mx_raS = new_df_event.withColumn( "second", second($"created_at"))
    val data_frame_max_repoactorS = df_mx_raS.select( $"second", $"repo", $"actor", count($"*").over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val df_max_repoactorS = data_frame_max_repoactorS.agg(max("conteggio"))
    df_max_repoactorS.show()
    //RDD_max
    val rdd_max_repoactorS = rdd_event.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_raS = rdd_max_repoactorS.map(x => x._2).max()
    println(rdd_max_raS)
    //DF_min
    val df_mn_raS = new_df_event.withColumn( "second", second($"created_at"))
    val data_frame_min_repoactorS = df_mn_raS.select( $"second", $"repo", $"actor", count($"*").over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val df_min_repoactorS = data_frame_min_repoactorS.agg(min("conteggio"))
    df_min_repoactorS.show()
    //RDD_min
    val rdd_min_repoactorS = rdd_event.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_raS = rdd_min_repoactorS.map(x => x._2).min()
    println(rdd_min_raS)

    //TODO: 3.1)contare il numero di commit
    //DF
    val df_nComit = commits_df.distinct().count()
    println(df_nComit)
    //RDD
    val rdd_nCommit = rdd_commit.distinct().count()
    println(rdd_nCommit)

    //TODO: 3.2)contare il numero di commit per actor


  }
}