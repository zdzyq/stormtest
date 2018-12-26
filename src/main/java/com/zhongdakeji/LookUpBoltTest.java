package com.zhongdakeji;

import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisLookupBolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * stormtest com.zhongdakeji
 * <p>
 * Create by zyq on 2018/12/19 0019
 */

//实时统计商品的销量，每个品类的实时销量
//日志数据：orderItemId，orderId,productId,number,amount，price
//需要redis里面有产品和category的对应信息，保存在：productCategory里面，类型是HASH
public class LookUpBoltTest {

    public static class GetStreamFromFileSpout extends BaseRichSpout {
        private SpoutOutputCollector spoutOutputCollector;
        private BufferedReader reader;
        private final String fileName = "F:\\zyq\\spark\\testdata\\ch04_data_products.txt";
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
            try {
                reader = new BufferedReader(new FileReader(fileName));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        public void nextTuple() {
            try {
                String line = reader.readLine();
                if(line != null){
                    String[] splits = line.split("\\|");
                    List tuple = Arrays.asList(splits);//List[String]
                    spoutOutputCollector.emit(tuple);//List[Object]
                }
            } catch (IOException e) {
                System.out.println("文件已读完");
                e.printStackTrace();
            }
        }
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("itemId","orderId","productId","quantity","subtotal","pirce"));
        }
    }
    //根据productId，去redis中查找其所属的categoryId，然后放到数据中后续进行分组聚合计算
    public static class GetCategoryByProductMapper implements RedisLookupMapper {
        private RedisDataTypeDescription dataTypeDescription;

        public GetCategoryByProductMapper() {
            //productCategory是redis中的数据的总key
            this.dataTypeDescription = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH,"productCategory");
        }
        //把从redis中获取到的数据，连同当前流处理中的tuple一起定义一个该bolt的输出内容
        //iTuple输入，o来自于redis中的查找====》输出
        public List<Values> toTuple(ITuple iTuple, Object o) {
            String quantity = iTuple.getStringByField("quantity");
            String categoryId = (String)o;
            List<Values> result = new ArrayList<Values>();
            result.add(new Values(categoryId,quantity));
            return result;
        }
        //输出的模式声明
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("categoryId","quantity"));
        }
        //定义从redis查找的value的类型说明
        public RedisDataTypeDescription getDataTypeDescription() {
            return dataTypeDescription;
        }
        //当去查找的时候需要提供的filed的定义
        public String getKeyFromTuple(ITuple iTuple) {
            return iTuple.getStringByField("productId");
        }

        public String getValueFromTuple(ITuple iTuple) {
            return null;
        }
    }
    //补全计算每个category的累计销量
    public static class CategoryCountBolt extends BaseBasicBolt {
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            System.out.println("categoryId:"+tuple.getStringByField("categoryId")+"----quantity:"+tuple.getStringByField("quantity"));
        }
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }
    public static void main(String[] args){
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("fromfile",new GetStreamFromFileSpout());
        JedisPoolConfig config = new JedisPoolConfig.Builder().setHost("bigdata01").setPort(6379).build();
        RedisLookupBolt lookupBolt = new RedisLookupBolt(config,new GetCategoryByProductMapper());
        tb.setBolt("lookupfromredis", lookupBolt).shuffleGrouping("fromfile");
        //计算累计销量
//        tb.setBolt("countquantity",new CategoryCountBolt()).shuffleGrouping("lookupfromredis");

        //使用redis的组合计算累计销量并保存到redis中,状态数据保存在redis的key：categorySaleState，类型Hash
        //1.从redis中查找是否已有某categoryid的累计销量，如果有取出来累计到我的销量字段中去
        //  如果没有则不用管
        RedisLookupBolt lookupState = new RedisLookupBolt(config, new RedisLookupMapper() {
            public List<Values> toTuple(ITuple iTuple, Object o) {
                String nowCategoryId = iTuple.getStringByField("categoryId");
                String nowQuantity = iTuple.getStringByField("quantity");
                if(o!=null && !"".equals(o)){
                    Integer stateQuantity = Integer.valueOf((String)o);
                    nowQuantity = String.valueOf(Integer.valueOf(nowQuantity)+stateQuantity);//取出累加更新到quantity字段后输出
                }
                List<Values> result = new ArrayList<Values>();
                result.add(new Values(nowCategoryId, nowQuantity));
                return result;
            }
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields("categoryId","quantity"));
            }
            public RedisDataTypeDescription getDataTypeDescription() {
                RedisDataTypeDescription dataTypeDescription = new RedisDataTypeDescription(
                        RedisDataTypeDescription.RedisDataType.HASH, "categorySaleState"
                );
                return dataTypeDescription;
            }
            //去redis中查找的key设置
            public String getKeyFromTuple(ITuple iTuple) {
                return iTuple.getStringByField("categoryId");
            }
            public String getValueFromTuple(ITuple iTuple) {
                return null;
            }
        });
        tb.setBolt("getstatefromredis",lookupState).fieldsGrouping("lookupfromredis",new Fields("categoryId"));
        //2.把结果保存到redis中
        RedisStoreBolt stateStoreBolt = new RedisStoreBolt(config, new RedisStoreMapper() {
            public RedisDataTypeDescription getDataTypeDescription() {
                RedisDataTypeDescription dataTypeDescription = new RedisDataTypeDescription(
                        RedisDataTypeDescription.RedisDataType.HASH, "categorySaleState"
                );
                return dataTypeDescription;
            }
            public String getKeyFromTuple(ITuple iTuple) {
                return iTuple.getStringByField("categoryId");
            }
            public String getValueFromTuple(ITuple iTuple) {
                return iTuple.getStringByField("quantity");
            }
        });
        tb.setBolt("savestatetoredis",stateStoreBolt).shuffleGrouping("getstatefromredis");

        LocalCluster local = new LocalCluster();
        local.submitTopology("lookupredis",new HashMap(),tb.createTopology());
    }

}
