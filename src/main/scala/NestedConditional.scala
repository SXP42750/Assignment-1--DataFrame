import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.{SparkConf, SparkContext}

object NestedConditional {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")

    inventory.withColumn("stock_level", when(col("quantity") < 10, "low").when(col("quantity") > 10 && col("quantity") < 20,"Medium").otherwise("High")
    ).show()
  }
}