import java.sql.PreparedStatement

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import util.JdbcUtils

object TestScala {

  def main(args: Array[String]): Unit = {
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("KafkaOffsetDemo").setMaster("local[4]")
    // 创建StreamingContext batch size 为 1秒
    val sc = new SparkContext(sparkConf)

    try {
      sort(sc)
        //  select(sc)
     // mysql(sc)
    }finally {
      sc.stop()
    }



  }

  /**
    * 北京飞往重庆航空公司的数量
    * @param sc
    */
  def select(sc: SparkContext): Unit = {
     val rdd= sc.textFile("国内航班数据500条.csv")
    .filter(lime => {
        lime.split(",")(0) == "北京" && lime.split(",")(3) == "重庆"
      })
      .map(lime => lime.split(",")(8))
      .distinct()

     rdd.saveAsTextFile("out/select1")
     // .foreach(lime => println(lime))

    val number = sc.textFile("国内航班数据500条.csv")
      .filter(lime => {
        lime.split(",")(0) == "北京" && lime.split(",")(3) == "重庆"
      })
      .map(lime => lime.split(",")(8))
      .distinct()
      .count()
    println("有" + number + "个航空公司有从北京飞往重庆的航班")
    printf("select的分区数为%d", rdd.partitions.length)
  }

  /**
    * 排出航班数最多的航空公司的前6名
    * @param sc
    */
  def sort(sc: SparkContext): Unit = {
   val rdd= sc.textFile("国内航班数据500条.csv")
      .map(lime => (
        lime.split(",")(8),1))
      .reduceByKey((x, y) => {
        x + y
      })
      .map(lime => {
        (lime._2,lime._1)
      })
     .partitionBy(new HashPartitioner(4))
    //rdd.top(6).foreach(lime => println(lime))
    rdd.foreach(lime => println(lime))
    rdd.saveAsTextFile("out/sort1")

   printf("sort的分区数为%d", rdd.partitions.length)
  }

  def mysql(sc: SparkContext): Unit ={
    val rdd= sc.textFile("国内航班数据500条.csv")
      .map(lime => (
        lime.split(",")(8),1))
      .reduceByKey((x, y) => {
        x + y
      })
      .map(lime => {
        (lime._2,lime._1)
      })
      .partitionBy(new HashPartitioner(4))
      .foreachPartition(partition =>{
        val conn =JdbcUtils.getConnection()
        var pstmt :PreparedStatement= null
       partition.foreach(lime =>{
        val sql = "insert into datademo (name,number) values(?,?)"
        pstmt = conn.prepareStatement(sql)
        pstmt.setString(1,lime._2)
         pstmt.setString(2,lime._1.toString)
        pstmt.executeUpdate
      })
        JdbcUtils.free(pstmt, conn)
    }
    )



  }



}
