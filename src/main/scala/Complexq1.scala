import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.{SparkConf, SparkContext}

object Complexq1 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 40000)
    ).toDF("employee_id", "age", "salary")

    employees.withColumn("category", when(col("age") < 30 && col("salary") < 35000, "Young & Low Salary").when(col("age") > 30 && col("age") < 40 && col("Salary") > 35000 && col("Salary") < 45000, "Middle Aged & Medium Salary").otherwise("Old & High Salary")
    ).show(false)
  }
}
