package com.wjl.apiTest.transform;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TransformTest2_Aggregation {
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
    //  KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(data->data.getId());
    //  KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
//        keyedStream.print("key");
        // 输出最大的温度值并且其对应的时间戳
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        // 打印输出
        resultStream.print("result");
        env.execute();

    }
}
