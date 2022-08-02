package com.wjl.apiTest.sink;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class sinkTest1_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个分区输出一次open和close
        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });
        dataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092","sinkTest",new SimpleStringSchema()));
        env.execute();
    }


}

