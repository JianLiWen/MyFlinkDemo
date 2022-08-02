package com.wjl.apiTest.transform;

import com.mysql.fabric.xmlrpc.base.Data;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        // 1.map String 换成长度输出
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        // flatmap 按照逗号分割
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for(String field:fields){
                    collector.collect(field);
                }
            }
        });

        // filter 筛选sensor1开头的id对应的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        env.execute();

    }
}
