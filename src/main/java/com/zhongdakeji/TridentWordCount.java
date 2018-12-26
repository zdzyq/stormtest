package com.zhongdakeji;

import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * stormtest com.zhongdakeji
 * <p>
 * Create by zyq on 2018/12/21 0021
 */
public class TridentWordCount {
    public static void main(String[] args){
        //声明spout
        Fields fields = new Fields("line");
        FixedBatchSpout spout = new FixedBatchSpout(fields,5,
                new Values("This distribution includes cryptographic software.  The country in "),
                new Values("which you currently reside may have restrictions on the import"),
                new Values("The following provides more details on the included cryptographic"),
                new Values("ActiveMQ supports the use of SSL TCP connections when used with "),
                new Values("check your country's laws, regulations and policies concerning the"),
                new Values("import, possession, or use, and re-export of encryption software, to ")
        );
        spout.setCycle(true);//设置循环产生数据

        //声明Topology
        TridentTopology tridentTopology = new TridentTopology();
        Stream stream = tridentTopology.newStream("textstream", spout);
        Stream result = stream.flatMap(new FlatMapFunction(){
            //把一条记录变成多条记录
            public Iterable<Values> execute(TridentTuple tridentTuple) {
                String[] splits = tridentTuple.getStringByField("line").split("\\s+");
                List<Values> multiResult = new ArrayList<Values>();
                for(String word : splits){
                    multiResult.add(new Values(word));
                }
                return multiResult;
            }
        },new Fields("word"))
                .groupBy(new Fields("word"))
                .aggregate(
//                  new Aggregator<Integer>(){
////                    private String key = null;
//                    private Integer count;
//                    public void prepare(Map map, TridentOperationContext tridentOperationContext) {
//                    }
//                    public void cleanup() {
//                    }
//                    public Integer init(Object o, TridentCollector tridentCollector) {
//                        count = 0;
//                        return 0;
//                    }
//                    public void aggregate(Integer integer, TridentTuple tridentTuple, TridentCollector tridentCollector) {
//                        count += 1;
//                    }
//                    public void complete(Integer integer, TridentCollector tridentCollector) {
//                        List result = new ArrayList();
////                        result.add(key);
//                        result.add(count);
//                        System.out.println(integer+"----------------------------");
//                        tridentCollector.emit(result);
//                    }
//                }

                        new CombinerAggregator<Integer>() {
                            public Integer init(TridentTuple tuple) {
                                return 1;
                            }
                            public Integer combine(Integer val1, Integer val2) {
                                return val1+val2;
                            }
                            public Integer zero() {
                                return 0;
                            }
                        }

//                        new Count()//这个方法的输出结果类型是long不是integer

                        , new Fields("count"))
                .each(new Fields("word","count"),new Function(){
                    public void prepare(Map map, TridentOperationContext tridentOperationContext) {
                    }
                    public void cleanup() {
                    }
                    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
                        String word = tridentTuple.getStringByField("word");
                        Integer count = tridentTuple.getIntegerByField("count");
                        System.out.println("word:"+word+"---------------"+"count:"+count);
                        tridentCollector.emit(new Values(word,count));
                    }
                },new Fields());


        LocalCluster local = new LocalCluster();
        local.submitTopology("trident",new HashMap(), tridentTopology.build());
    }
}
