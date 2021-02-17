import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.test.TestHive.emptyDataFrame.rdd
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.Environment.list

object JsonParser{

  val conf = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val input = "D:\\BigData\\ProgettoBigData\\2018-03-01-0.json"

  def main(args: Array[String]){
    val df = sqlContext.read.json(input)
    df.registerTempTable("jsonExtract")
    //rdd = df.rdd.map(list)
    //intera tabella
    val data0 = sqlContext.sql("select * from jsonExtract")
    data0.show()
    //singoli actor
    val data1 = sqlContext.sql("select actor from jsonExtract")
    data1.show()
    //singoli repo
    val data3 = sqlContext.sql("select repo from jsonExtract")
    data3.show()
    //singoli type
    val data4 = sqlContext.sql("select type from jsonExtract")
    data4.show()
    //numero actor
    val data5 = sqlContext.sql("select count(actor) from jsonExtract")
    data5.show()
    //numero repo
    val data6 = sqlContext.sql("select count(repo) from jsonExtract")
    data6.show()
    sc.stop
  }
}