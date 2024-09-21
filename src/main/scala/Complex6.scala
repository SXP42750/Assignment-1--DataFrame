import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.{SparkConf, SparkContext}

object Complex6 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val weather = List(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ).toDF("day_id", "temperature", "humidity")

    weather.withColumn("is_hot", when(col("temperature") > 30, true).otherwise(false)
    ).withColumn("is_humid",when(col("humidity") > 40 ,true).otherwise(false)
    ).show()
  }
}