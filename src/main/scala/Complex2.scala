import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.{SparkConf, SparkContext}

object Complex2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")

     val result = reviews.withColumn("feedback", when(col("rating") < 3, "Bad").when(col("rating") === 3 || col("rating") === 4, "Good").otherwise("Excellent")
    ).show()
    reviews.withColumn("_positive",when(col("rating") >= 3 , true).otherwise(false)
    ).show()
  }
}

