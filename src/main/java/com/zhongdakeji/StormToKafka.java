package com.zhongdakeji;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * stormtest com.zhongdakeji
 * <p>
 * Create by zyq on 2018/12/19 0019
 */
public class StormToKafka {
    //定义一个spout，随机产生用户登录操作日志或者用户退出日志
    //用户id，操作时间，操作类型（0登录/1退出/2搜索/3下单/4支付）
    //userId,opDate,opType
    public static class RandomActionLogSpout extends BaseRichSpout {
        private SpoutOutputCollector spoutOutputCollector;
        private Random random = new Random();
        private String[] actionTypes = new String[]{"0","1","2","3","4"};
        private List tuple = new ArrayList();
        //用户id，1~100随机
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }
        public void nextTuple() {
            tuple.clear();
            //随机一条数据并发射出去
            String actionType = actionTypes[random.nextInt(actionTypes.length)];
            String userId = "user_" + (random.nextInt(100)+1);
            tuple.add(userId);
            tuple.add(new Date().getTime());
            tuple.add(actionType);
            System.out.println(tuple.get(2)+"------------------------------");
            spoutOutputCollector.emit(tuple);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("userId","opDate","opType"));
        }
    }
    //过滤掉行为类型非0、1的数据（tuple）
    public static class FilterByActionTypeBolt extends BaseRichBolt {
        private OutputCollector outputCollector;
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }
        //过滤
        public void execute(Tuple tuple) {
            if(tuple.getStringByField("opType")!=null && (tuple.getStringByField("opType").equals("0")||tuple.getStringByField("opType").equals("1"))){
                List outTuple = new ArrayList();
                outTuple.add(tuple.getStringByField("userId"));
                outTuple.add(tuple.getLongByField("opDate"));
                outTuple.add(tuple.getStringByField("opType"));
                outputCollector.emit(outTuple);
            }
        }
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("userId","opDate","opType"));
        }
    }
    //使用KafkaBolt把数据存放到kafka中
    public static void main(String[] args){
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("randomactionlog",new RandomActionLogSpout(),2);
        tb.setBolt("filterbyatype",new FilterByActionTypeBolt(),2).shuffleGrouping("randomactionlog");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","bigdata01:9092,bigdata02:9092,bigdata03:9092");
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt kafkaBolt = new KafkaBolt<String,String>()
                .withProducerProperties(properties)
                .withTupleToKafkaMapper(new TupleToKafkaMapper<String,String>(){
                    public String getKeyFromTuple(Tuple tuple) {
                        return tuple.getStringByField("userId");
                    }
                    public String getMessageFromTuple(Tuple tuple) {
                        return tuple.getLongByField("opDate")+","+tuple.getStringByField("opType");
                    }
                })
                .withTopicSelector(new DefaultTopicSelector("test_topic"));

        tb.setBolt("tokafka",kafkaBolt ,2).shuffleGrouping("filterbyatype");

        //本地启动topology
        Config config = new Config();
        config.setDebug(true);
        LocalCluster local = new LocalCluster();
        local.submitTopology("tokafka", config, tb.createTopology());
    }
}
