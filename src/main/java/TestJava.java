import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Objects;

/**
 * 类描述：
 *
 * @author:maimanchuang
 * @date 2018/12/2018/12/19
 */
public class TestJava {


    public static void main(String[] agr){
     SparkConf sparkConf=new SparkConf().setAppName("sss").setMaster("local[4]");
     JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd= sparkContext.textFile("国内航班数据500条.csv");
      rdd.mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) throws Exception {
              return new Tuple2<>(s.split(",")[8], 1);
          }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
              return v1+v2;
          }
      }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
          @Override
          public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
              System.out.println(stringIntegerTuple2._1+":"+stringIntegerTuple2._2);
          }
      });


     ;


    }

}
