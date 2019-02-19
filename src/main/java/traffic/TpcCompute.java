package traffic;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import util.MapUtil;
import util.TestDataUtil;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * 套牌车分析算法实现
 * brave
 */
public class TpcCompute {
    public static void main(String[] args){
        SparkSession spark=SparkSession.builder().enableHiveSupport().appName("TpcCompute").master("local").getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");

//        Dataset<Row> cltgxxDF = spark.read().option("header", "true").csv("file:///D:\\idea2018workspace\\cnw_spark_svn\\in\\tpc.csv");
//        cltgxxDF.createOrReplaceTempView("t_cltgxx");
        Dataset<Row> cltgxxDF = spark.sql("select * from t_cltgxx t where t.tgsj>'2017-03-24 09:22:22'");
        cltgxxDF.show();
        /**
         * +------+-------+----+----+----+----+-------------------+--------+----+--------------------+
         * |    ID|   HPHM|HPZL|HPYS|CLPP|CLYS|               TGSJ|    KKBH|TPDZ|          KK_LON_LAT|
         * +------+-------+----+----+----+----+-------------------+--------+----+--------------------+
         * |997143|粤AL9786|小型汽车|蓝底白字|  大众|   白|2018-09-26 17:35:00|kk822221|null|114.035958_33.670271|
         */
        //拆分数据为(车牌号：唯一标识_卡口经度坐标_卡口纬度坐标_通过时间)
        JavaRDD<Row> resultRowRDD = cltgxxDF.javaRDD();
        JavaPairRDD<String, String> pairRDD = resultRowRDD.mapToPair(new PairFunction<Row,String,String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String HPHM=row.getAs("hphm");
                String ID=row.getAs("id");
                String TGSJ=((Timestamp)row.getAs("tgsj")).toString();
                String KK_LON_LAT=row.getAs("kk_lon_lat");
                return new Tuple2<String, String>(HPHM,ID+"_"+KK_LON_LAT+"_"+TGSJ);
            }
        });//(车牌号：唯一标识_卡口经度坐标_卡口纬度坐标_通过时间)

        //使用reducebykey算子合并同车牌号数据
        JavaPairRDD<String, String> reduceRDD = pairRDD.reduceByKey(new Function2<String, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1+"&"+v2;
            }
        });//(车牌号：唯一标识_卡口经度坐标_卡口纬度坐标_通过时间&唯一标识_卡口经度坐标_卡口纬度坐标_通过时间&......)
        //创建集合累加器
        CollectionAccumulator<String> acc = sc.sc().collectionAccumulator();
        //使用foreach算子过滤数据，将符合条件的数据放到累加器中
        reduceRDD.foreach(new VoidFunction<Tuple2<String,String>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, String> t2) throws Exception {
                //粤AL9786
                String hphm=t2._1;
                //"ID+"_"+lon+"_"+lat+"_"+tgsj & ID+"_"+lon+"_"+lat+"_"+tgsj"
                String value=t2._2;
                //[ID+"_"+lon+"_"+lat+"_"+tgsj]
                String[] values = value.split("&");
                //遍历values 进行业务逻辑处理-------开发人员编写业务逻辑处理代码
                for(int i=0;i<values.length;i++){
                    for(int k=i+1;k<values.length;k++){
                        String value1=values[i];
                        String value2=values[k];
                        String[] items1=value1.split("_");
                        String[] items2=value2.split("_");
                        String id1=items1[0];
                        String lon1=items1[1];
                        String lat1=items1[2];
                        String tgsj1=items1[3];

                        String id2=items2[0];
                        String lon2=items2[1];
                        String lat2=items2[2];
                        String tgsj2=items2[3];

                        double subHour= TestDataUtil.getSubHour(tgsj1, tgsj2);
                        double distance= MapUtil.getLongDistance(Double.valueOf(lon1), Double.valueOf(lat1),Double.valueOf(lon2),Double.valueOf(lat2));
                        Integer speed = TestDataUtil.getSpeed(distance, subHour);
                        if(speed>180){
                            //如果车牌号相同的两车,
                            // 卡口时间相差值，卡口距离值====>速度大于180km/h，则为套牌车。或者
                            // 卡口时间相差小于等于5分钟，同时两车卡口距离大于10KM(即速度大于120km/h)，则为套牌车
                            acc.add(id1+"_"+id2);//符合条件的添加到累加器中
                        }
                    }
                }
            }
        });

        List<String> accValue = acc.value();
        for(String id : accValue){
            System.out.println("accValue: "+id);
            Dataset<Row> resultDF = spark.sql("select hphm,clpp,clys,tgsj,kkbh from t_cltgxx where id in (" + id.split("_")[0] + "," + id.split("_")[1] + ")");
            resultDF.show();
            //jsbh计算编号，create_time创建时间
            Dataset<Row> resultDF2 = resultDF.withColumn("jsbh", functions.lit(new Date().getTime()))
                                             .withColumn("create_time", functions.lit(new Timestamp(new Date().getTime())));
            resultDF2.write()
                    .format("jdbc")
                    //?characterEncoding=UTF-8避免中文乱码
                    .option("url", "jdbc:mysql://bigdata03:3306/test?characterEncoding=UTF-8")
                    .option("dbtable", "t_tpc_result")
                    .option("user", "root")
                    .option("password", "123456")
                    .mode(SaveMode.Append)
                    .save();
        }
    }

}
