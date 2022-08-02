package com.wjl.apiTest.transform;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TransformTest3_reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        // 转换成sensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map(line->{
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // reduce 聚合，取最大的温度值，并输出当前时间戳
        DataStream<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            // v2是最新的值，v1是最新的状态
            public SensorReading reduce(SensorReading v1, SensorReading v2) throws Exception {
                return new SensorReading(v1.getId(), v2.getTimeStamp(), Math.max(v1.getTemperature(),v2.getTemperature()));
            }
        });
        // 打印输出
        reduceStream.print();
        env.execute();

    }
}
