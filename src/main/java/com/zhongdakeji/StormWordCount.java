package com.zhongdakeji;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
public class StormWordCount {
    //随机的产生并发送一些字符串作为流数据
    public static class RandomStringSpout extends BaseRichSpout {
        private SpoutOutputCollector spoutOutputCollector;
        private Random random = new Random();
        private List<String> strsForRandom = new ArrayList<String>();
        private List msgForEmit = new ArrayList();  //代替元组类型来封装数据
        //初始化spout的方法
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
            strsForRandom.add("Limitation of Liability. In no event and under no legal theory");
            strsForRandom.add("liable to You for damages, including any direct, indirect, special");
            strsForRandom.add("result of this License or out of the use or inability to use the");
            strsForRandom.add("work stoppage, computer failure or malfunction, or any and all");
            strsForRandom.add("Accepting Warranty or Additional Liability. While redistributing");
            strsForRandom.add("the Work or Derivative Works thereof, You may choose to offer");
        }
        //产生流数据的方法，每当该方法被调用一次之后就会往topology里面发送（emit）一些数据（tuple）
        public void nextTuple() {
            String str = strsForRandom.get(random.nextInt(strsForRandom.size()));
            msgForEmit.clear();
            msgForEmit.add(str);
            spoutOutputCollector.emit(msgForEmit);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //给输出元组声明模式
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }
    //bolt把spout的每一行数据分割成一个一个单词，每个单词输出一次
    public static class SplitLineBolt extends BaseRichBolt {
        private OutputCollector outputCollector;
        private List list = new ArrayList();
        //bolt初始化时执行的方法
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }
        //计算的业务逻辑执行方法，输出也在这里执行
        public void execute(Tuple tuple) {
            //从tuple取出输入数据
            String line = tuple.getStringByField("line");
            for(String word:line.split("\\s+")){
                list.clear();
                list.add(word);
                list.add(1);
                outputCollector.emit(list);
            }
        }
        //输出元组的模式声明方法
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word","count"));
        }
    }
    //bolt统计每个单词的数量出现的次数
    public static class CountWordBolt extends BaseRichBolt {
        private Map<String,Integer> wc = new HashMap<String, Integer>();
        private String word;
        private Integer count;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        }
        //count的业务逻辑
        public void execute(Tuple tuple) {
            word = tuple.getStringByField("word");
            count = tuple.getIntegerByField("count");
            if(wc.containsKey(word)){
                wc.put(word,count+wc.get(word));
            }else{
                wc.put(word,count);
            }

            for(String word:wc.keySet()){
                System.out.println("word:"+word+",count:"+wc.get(word));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        //创建toplogybuild对象，该对象用来组装toplogy
        TopologyBuilder tb = new TopologyBuilder();
        //设定spout
        tb.setSpout("randomSpout",new RandomStringSpout(),2); //2代表并行度，代表同时有两个进程（或两个节点）启动运行spout的搜集数据服务
        tb.setBolt("splitBolt",new SplitLineBolt(),2).shuffleGrouping("randomSpout"); //最后一个参数代表流数据来源的组件id
        tb.setBolt("countBolt",new CountWordBolt(),2).fieldsGrouping("splitBolt",new Fields("word"));

        //提交发布运行toplogy
        //1.本地运行
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("wordcount", new HashMap(), tb.createTopology());
        //集群运行
        Config conf = new Config();
        conf.setDebug(true);
        StormSubmitter.submitTopology("wc",conf,tb.createTopology());
    }
}
