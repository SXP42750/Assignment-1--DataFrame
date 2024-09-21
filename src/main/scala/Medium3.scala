import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.{SparkConf, SparkContext}

object Medium3 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    orders.withColumn("season", when(month(col("order_date")) === 6 || month(col("order_date")) === 7 || month(col("order_date")) === 8, "Summer").when(month(col("order_date")) === 12 || month(col("order_date")) === 1|| month(col("order_date")) === 2, "Winter").otherwise("Others")
    ).show()
  }
  }

