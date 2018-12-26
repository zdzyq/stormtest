package com.zhongdakeji;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * stormtest com.zhongdakeji
 * <p>
 * Create by zyq on 2018/12/19 0019
 */
//在线人数
public class StormFromKafka {
    //bolt统计在线人数
    public static class OnlineUserCountBolt extends BaseBasicBolt {
        private int onlineNumber;
        //kafka的spout输出的元组的模式是："topic", "partition", "offset", "key", "value"
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String topic = tuple.getStringByField("test_topic");
            Integer partition = tuple.getIntegerByField("partition");
            Long offset = tuple.getLongByField("offset");
            String key = tuple.getStringByField("key");
            String value = tuple.getStringByField("value");
//            System.out.println(topic+"--"+partition+"--"+offset+"--"+key+"--"+value);
            if(value!=null && value.split(",")[1].equals("0")){
                onlineNumber += 1;
            }else{
                onlineNumber -= 1;
            }
            System.out.println("--------当前在线人数："+onlineNumber);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }
    public static void main(String[] args){
        TopologyBuilder tb = new TopologyBuilder();
        //定义kafkaspout
        KafkaSpoutConfig config = KafkaSpoutConfig.builder("bigdata01:9092,bigdata02:9092,bigdata03:9092","test_topic")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,"stormconsumer")
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//                .setRecordTranslator() //用来把kafka的message转换成storm的tuple的定义
                .build();
        KafkaSpout kafkaSpout = new KafkaSpout(config);
        tb.setSpout("kafkaspout", kafkaSpout);
        tb.setBolt("onlinecount", new OnlineUserCountBolt()).shuffleGrouping("kafkaspout");

        //本地启动
        LocalCluster local = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        local.submitTopology("count", conf, tb.createTopology());

    }
}
