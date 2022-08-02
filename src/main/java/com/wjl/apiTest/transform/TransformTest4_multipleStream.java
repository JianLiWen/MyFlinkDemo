package com.wjl.apiTest.transform;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collection;
import java.util.Collections;


public class TransformTest4_multipleStream {
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

        // 1.分流，按照30度进行分流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high","low");

        // 打印输出
        highStream.print("high");
        lowStream.print("low");
        allStream.print("all");
        env.execute();

        // 合流connect，将高温流转换成二元组类型，与低温流连接合并，s输出状态信息
        DataStream<Tuple2<String,Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String,Double>>() {

            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(),sensorReading.getTemperature());
            }
        });

        // connectstream数据类型可以不一致，但是只能二合一
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowStream);
        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp waring");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "low temp waring");
            }
        });

        // 联合多条流,union可以连接多个流，但是数据类型必须一致
        highStream.union(lowStream);
        resultStream.print();
        env.execute();
    }
}
