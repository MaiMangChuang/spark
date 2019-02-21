package traffic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * 套牌车布控算法实现
 * author:brave
 */
public class TpcStreamingCompute2 {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("ScrcStreamingCompute").getOrCreate();

        Dataset<Row> tpcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://hadoop03:3306/test?characterEncoding=UTF-8")
                .option("dbtable", "t_tpc_result")
                .option("user", "root")
                .option("password", "123456")
                .load();
        Dataset<Row> tpcHphmDF = tpcDF.select("hphm").distinct();
        tpcHphmDF.cache();//提高性能
        tpcHphmDF.show();
        JavaRDD<Row> tpcRDD = tpcHphmDF.javaRDD();
        JavaPairRDD<String, String> tpcPairRDD = tpcRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String hphm = row.getAs("hphm");
                return new Tuple2<String, String>(hphm, hphm);
            }
        });
        spark.sparkContext().setLogLevel("ERROR");

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        String brokers = "hadoop01:9092";

        String topics = "cnwTopic";

        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(6));

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "tpc_group");

        kafkaParams.put("auto.offset.reset", "latest");

        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
//        offsets.put(new TopicPartition("cnwTopic", 0), 2L);


        final JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );
        kafkaDStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
                if (rdd.count() > 0) {
                    System.out.println("打印从kafka中获取的数据...");
//                    rdd.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
//                        @Override
//                        public void call(ConsumerRecord<String, String> consumerRecord) throws Exception {
//                            System.out.println(consumerRecord.value());
////                            consumerRecord.offset();
//                        }
//                    });
                    System.out.println("打印offset信息");
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                        @Override
                        public void call(Iterator<ConsumerRecord<String, String>> iter) throws Exception {
                            OffsetRange offsetRange = offsetRanges[TaskContext.get().partitionId()];
                            System.out.println("topic:" + offsetRange.topic()
                                    + ",partition:" + offsetRange.partition()
                                    + ",fromOffset:" + offsetRange.fromOffset()
                                    + ",untilOffset:" + offsetRange.untilOffset());
                        }
                    });
                    ((CanCommitOffsets) kafkaDStream.inputDStream()).commitAsync(offsetRanges);//Java语言--此行代码有问题
                }
            }
        });

        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}
