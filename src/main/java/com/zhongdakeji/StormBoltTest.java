package com.zhongdakeji;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.ITuple;

/**
 * stormtest com.zhongdakeji
 * <p>
 * Create by zyq on 2018/12/19 0019
 */
public class StormBoltTest {
    //定义storedmapper用来声明bolt中的tuple如何与redis中的kv对应
    public static class TextToRedisSetMapper implements RedisStoreMapper {
        //用来声明从storm把数据保存到redis里面的value的数据类型的描述对象
        private RedisDataTypeDescription dataTypeDescription;
        //把数据保存到redis中的key的值
        final String key = "storm_to_redis_key";

        //在构造方法中初始化属性
        public TextToRedisSetMapper() {
            dataTypeDescription = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.SET
            );//如果需要保存到redis中的数据是hash、storedset、geo等类型，则需要额外提供一个key值
        }
        public RedisDataTypeDescription getDataTypeDescription() {
            return dataTypeDescription;
        }
        //写出到redis中的数据映射定义key
        public String getKeyFromTuple(ITuple iTuple) {
            return key;
        }
        //输出到redis中的value
        public String getValueFromTuple(ITuple iTuple) {
            return iTuple.getStringByField("line");
        }
    }
    //从RandomStringSpout中获取输入
    //使用RedisStoreBolt把数据存储到redis中
    //key：stormset type:set
    public static void main(String[] args){
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("randomtext", new StormWordCount.RandomStringSpout());

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder().setHost("bigdata01")
                .setPort(6379).setDatabase(1)
                .build();


        RedisStoreBolt redisStoreBolt = new RedisStoreBolt(jedisPoolConfig, new TextToRedisSetMapper());

        tb.setBolt("storetoredis", redisStoreBolt).shuffleGrouping("randomtext");

        LocalCluster local = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        local.submitTopology("toredis", conf, tb.createTopology());
    }
}
