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
    public static void main(String[] args)throws InterruptedException{
        SparkSession spark=SparkSession.builder().master("local[2]").appName("ScrcStreamingCompute").getOrCreate();
        //获取疑似套牌车数据t_tpc_result
        Dataset<Row> tpcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://bigdata03:3306/test?characterEncoding=UTF-8")
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
        //接着使用spark streaming获取kafka中的数据
        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
        //kafka broker地址，多个地址用逗号分隔
        String brokers = "bigdata01:9092";
        //kafka topic名称,多个名称用逗号分隔
        String topics = "cnwTopic";
        //创建流处理上下文对象;JavaStreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(6));
        //创建topic集合变量，用于KafkaUtils.createDirectStream方法
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //创建kafka相关参数变量，用于KafkaUtils.createDirectStream方法
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "tpc_group");
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从最新消息开始消费
        kafkaParams.put("auto.offset.reset","latest");
        //如果是true，则消费者的offset会在后台自动提交\
        kafkaParams.put("enable.auto.commit",false);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建Topic分区对象，用于KafkaUtils.createDirectStream方法
        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
//        offsets.put(new TopicPartition("cnwTopic", 0), 2L);
        /**
         * KafkaUtils老版本的用法，参见spark软件包中的example目录中的相关例子。
         * 本项目采用新版本的用法。
         * 通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定。
         * LocationStrategies(本地策略)
         * PreferConsistent方法：将分区数据尽可能均匀地分配给所有可用的Executor(绝大多数情况使用此方法)。
         * PreferBrokers方法：如果Executor和kafka broker在同一台机器上，用此方法，将优先将分区调度到kafka分区leader所在的主机上。
         * PreferFixed方法：分区之间的负荷有明显的倾斜，用此方法。允许指定一个明确的分区到主机的映射。
         *ConsumerStrategies(消费者策略)
         * 新的Kafka消费者API有许多不同的方式来指定主题。它们相当多的是在对象实例化后进行设置的。
         * ConsumerStrategies提供了一种抽象，即使spark任务重启，也能获得配置好的消费者信息。
         * ConsumerStrategies的Subscribe方法通过一个确定的集合来指定Topic
         * ConsumerStrategies的SubscribePattern方法允许你使用正则表达式来指定Topic。
         *
         */

        final  JavaInputDStream<ConsumerRecord<String,String>> kafkaDStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );
        kafkaDStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
                if(rdd.count()>0){
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
                            System.out.println("topic:"+offsetRange.topic()
                                                +",partition:"+offsetRange.partition()
                                                +",fromOffset:"+offsetRange.fromOffset()
                                                +",untilOffset:"+offsetRange.untilOffset());
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
