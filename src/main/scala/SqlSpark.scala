import org.apache.spark.sql.SparkSession

object SqlSpark {
  case class Air()
  def main(args: Array[String]): Unit = {
   val spark= SparkSession.builder()
     .appName("ss")
     .master("local[*]")
     .getOrCreate()
     val airDF= spark.read.csv("F:\\IDEA\\IntelliJ IDEA 2018\\spark\\国内航班数据500条.csv")
       .withColumnRenamed("_c8","name")
      airDF.createOrReplaceTempView("zzz")
    //airDF.select("COUNT(*)").as("number").orderBy("_c8").show()
      spark.sql("SELECT DISTINCT name FROM zzz ").show(40)
  }
}
