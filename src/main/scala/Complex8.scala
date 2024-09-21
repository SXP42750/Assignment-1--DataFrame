import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.{SparkConf, SparkContext}

  object Complex8 {
    def main(args: Array[String]): Unit = {
      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.Name", "ConditionalBasedOnString")
      sparkconf.set("spark.master", "local[*]")

      val spark = SparkSession.builder()
        .config(sparkconf)
        .getOrCreate()

      import spark.implicits._

      val emails = List(
        (1, "user@gmail.com"),
        (2, "admin@yahoo.com"),
        (3, "info@hotmail.com")
      ).toDF("email_id", "email_address")

      emails.withColumn("email_domain",when(col("email_address").contains("gmail"),"Gmail").when(col("email_address").contains("yahoo"),"Yahoo").otherwise("Hotmail")
      ).show()

    }
  }