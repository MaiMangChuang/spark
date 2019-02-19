package traffic;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.util.Date;

/**
 *首次入城车辆分析算法实现
 *brave
 */
public class ScrcCompute {
    public static void main(String[] args){
        SparkSession spark=SparkSession.builder().enableHiveSupport().appName("ScrcCompute").master("local").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");
        //指定时间段之前半年的过车数据
        Dataset<Row> allData = spark.sql("select * from t_cltgxx");
        //指定时间段的入城卡口过车数据(入城方向)
        Dataset<Row> inData = spark.sql("select * from t_cltgxx_in");

//        Dataset<Row> joinResult = inData.join(allData,
//                inData.col("hphm").equalTo(allData.col("hphm")),
//                                              "left");
//        joinResult.filter(allData.col("hphm").isNull())
//                  .select(inData.col("hphm"),inData.col("tgsj"),inData.col("kkbh")).show();

        Dataset<Row> resultDS = spark.sql("select t1.tgsj as tgsj,t1.hphm,t1.kkbh from t_cltgxx_in t1 LEFT OUTER JOIN t_cltgxx t2 on t1.hphm=t2.hphm where t2.hphm is null");
        Dataset<Row> resultDS2 = resultDS.withColumn("jsbh", functions.lit(new Date().getTime()))
                                         .withColumn("create_time",functions.lit(new Timestamp(new Date().getTime())));
        resultDS2.show();
        resultDS2.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://bigdata03:3306/test?characterEncoding=UTF-8")
                .option("dbtable", "t_scrc_result")
                .option("user", "root")
                .option("password", "123456")
                .mode(SaveMode.Append)
                .save();
    }
}
