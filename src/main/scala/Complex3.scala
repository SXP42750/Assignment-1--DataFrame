import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.{SparkConf, SparkContext}

object Complex3 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")

    documents.withColumn("content_category", when(col("content").contains("fox"), "Animal Related").when(col("content").contains("Lorem"), "Placeholder").otherwise("Tech Related")
    ).show()
  }
}