import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object JsonParser{

  val conf = new SparkConf().setAppName("Spark json extract")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val input = "D://BigData//ProgettoBigData/2018-03-01-0.json.gz"
  def main(args: Array[String]): Unit = {
    val df = sqlContext.read.json(input)
    df.registerTempTable("jsonExtract")
    val data = sqlContext.sql("select * from jsonExtract")
    data.show()
    sc.stop

  }
}
