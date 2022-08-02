package com.wjl.apiTest.state;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;
import scala.Int;

/**
 * @author wenjianli
 * @date 2022/2/20 7:25 下午
 */
public class StateTest3_keyedStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
//        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个flatmap操作，检测问题
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = dataStream.keyBy("id").flatMap(new TempChangeWarning(10.0));
        result.print();
        env.execute();
    }
        public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

            // 私有属性，温度跳变值
            private  Double threshold;
            public TempChangeWarning(Double threshold){
                    this.threshold = threshold;
            }

            // 定义状态，保存上次温度值
            private ValueState<Double> lastTempState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
            }

            @Override
            public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
                // 获取上次的温度值
                Double lastTemp = lastTempState.value();
                if (lastTemp != null){
                    Double diff = Math.abs(sensorReading.getTemperature()-lastTemp);
                    if (diff >= threshold){
                        collector.collect(new Tuple3<>(sensorReading.getId(),lastTemp,sensorReading.getTemperature()));
                    }
                }
                // 更新状态
                lastTempState.update(lastTemp);
            }

            @Override
            public void close() throws Exception {
                lastTempState.clear();
            }
        }


}




