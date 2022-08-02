package com.wjl.apiTest.transform;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.Tuple;
import scala.Int;
import scala.Tuple2;

public class TransformTest5_RichFunction  {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个分区输出一次open和close
        env.setParallelism(4);

        DataStream<String> dataStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        DataStream<Tuple2<String, Integer>> resultStream = mapStream.map(new MyMapper());
        resultStream.print();
        env.execute();


    }

    public static class MyMapper0 implements MapFunction<SensorReading,Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),sensorReading.getId().length());
        }
    }

    public static class MyMapper extends RichMapFunction<SensorReading,Tuple2<String,Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态或者建立数据库连接
            System.out.println("open");
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
            super.close();
        }
    }
}
