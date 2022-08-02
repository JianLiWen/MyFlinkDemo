package com.wjl.apiTest.transform;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个分区输出一次open和close
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<String> shuffleStream = inputStream.shuffle();
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        dataStream.print("input");
        // 随机分配到分区
        shuffleStream.print("shuffle");
        // 同一个key的分到一个分区
        dataStream.keyBy("id").print("keyby");
        // 全部弄到一个分区
        dataStream.global().print("global");
        env.execute();


    }

}
