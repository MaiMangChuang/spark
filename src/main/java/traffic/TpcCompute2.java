package traffic;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.CollectionAccumulator;

import java.util.List;

/**
 * 套牌车分析算法实现2
 * brave
 */
public class TpcCompute2 {
    public static void main(String[] args){
        SparkSession spark=SparkSession.builder().enableHiveSupport().appName("TpcCompute").master("local").getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");
        //hphm,id,tgsj,lonlat&
        spark.udf().register("getTpc",new ComputeUDF(), DataTypes.StringType);


        spark.sql("select hphm,getTpc(concat_ws('&',collect_set(concat_ws('_',id,kk_lon_lat,tgsj)))) as concatInfo from t_cltgxx t where t.tgsj>'2017-03-24 09:22:22' group by hphm").show(false);

        Dataset<Row> cltgxxDF = spark.sql("select hphm,concatInfo from (select hphm,getTpc(concat_ws('&',collect_set(concat_ws('_',id,kk_lon_lat,tgsj)))) as concatInfo from t_cltgxx t where t.tgsj>'2017-03-24 09:22:22' group by hphm) where concatInfo is not null");

        //创建集合累加器
        CollectionAccumulator<String> acc = sc.sc().collectionAccumulator();
        cltgxxDF.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                acc.add(row.getAs("concatInfo"));
            }
        });

        List<String> accValue = acc.value();
        for(String id : accValue){
            System.out.println(id);

        }
        //不要在map、foreach算子中调用 dataFrame 或 RDD 方法
        //注意：流处理里面的transform方法可以遍历RDD
//        cltgxxDF.foreach(new ForeachFunction<Row>() {
//            @Override
//            public void call(Row row) throws Exception {
//                String ids = row.getAs("concatInfo");
//                for(String id : ids.split("&")){
//                    System.out.println("accValue: "+id);
//                    System.out.println("sql: "+"select hphm,clpp,clys,tgsj,kkbh from t_cltgxx where id in (" + id.split("_")[0] + "," + id.split("_")[1] + ")");
//                    Dataset<Row> resultDF = spark.sql("select hphm,clpp,clys,tgsj,kkbh from t_cltgxx where id in (" + id.split("_")[0] + "," + id.split("_")[1] + ")");
//                    resultDF.show();
//                    Dataset<Row> resultDF2 = resultDF.withColumn("jsbh", functions.lit(new Date().getTime()))
//                            .withColumn("create_time", functions.lit(new Timestamp(new Date().getTime())));
//                    resultDF2.write()
//                            .format("jdbc")
//                            //?characterEncoding=UTF-8避免中文乱码
//                            .option("url", "jdbc:mysql://bigdata03:3306/test?characterEncoding=UTF-8")
//                            .option("dbtable", "t_tpc_result")
//                            .option("user", "root")
//                            .option("password", "123456")
//                            .mode(SaveMode.Append)
//                            .save();
//                }
//            }
//        });


    }

}
