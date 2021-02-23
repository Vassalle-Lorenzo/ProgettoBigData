import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Timestamp
import java.text.SimpleDateFormat

object Streamer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyFirstSparkapp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val streamc = new StreamingContext(sc, Seconds(5))

    val filestream = streamc.textFileStream("D:\\BigData\\stream\\inputfolder")
    streamc.checkpoint("D:\\BigData\\checkpoint")

    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    val logs = filestream.flatMap(line => {
      val lineSplitted = line.split(",")
      List(Log(new Timestamp(dateformat.parse(lineSplitted(0)).getTime), lineSplitted(1), lineSplitted(2).toInt))
    })

    val logsPair = logs.map(l => (l.tag, 1L))
    val result = logsPair.reduceByKey((l1, l2) => l1 + l2)

    val myFunction = (seqVal: Seq[Long], stateOpt: Option[Long]) => {
      stateOpt match {
        case Some(state) => Option(seqVal.sum + state)
        case None => Option(seqVal.sum)
      }
    }

    val logsPairTotal = logsPair.updateStateByKey(myFunction)
    val joinedLogsPair = result.join(logsPairTotal)

    //calcolare il numero di log per un tag, con finestra temporale di 20 secondi
    val resultWindow = logsPair.reduceByKeyAndWindow((l1,l2) => l1 + l2, Seconds(20))
    val joinedWindow = resultWindow.join(joinedLogsPair)
    joinedWindow.repartition(1).saveAsTextFiles("D:\\BigData\\stream\\outputfolder", "txt")

    streamc.start()
    streamc.awaitTermination()

  }
}