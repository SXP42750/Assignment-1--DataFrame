import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.{SparkConf, SparkContext}

object Complex7 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "MultipleConditional")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")

    scores.withColumn("math_grade", when(col("math_score") > 80, "A").when(col("math_score") > 60 && col("math_score") < 80, "B").otherwise("C")
    ).withColumn("english_grade", when(col("english_score") > 80, "A").when(col("english_score") > 60 && col("english_score") < 80, "B").otherwise("C")
    ).show()
  }
}