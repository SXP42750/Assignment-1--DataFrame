import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.{SparkConf, SparkContext}

object Complex9 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "ConditionalBasedOnString")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")

    payments.withColumn("quarter",when(month(col("payment_date")) === 1 || month(col("payment_date")) === 2 ||month(col("payment_date")) === 3 , "Q1").when(month(col("payment_date")) === 4 || month(col("payment_date")) === 5 || month(col("payment_date")) === 6 ,"Q2").when(month(col("payment_date")) === 7 || month(col("payment_date")) === 8 || month(col("payment_date")) === 9 , "Q3").otherwise("Q4")
    ).show()
  }
}
