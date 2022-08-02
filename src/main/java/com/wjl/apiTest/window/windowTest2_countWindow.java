package com.wjl.apiTest.window;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;

public class windowTest2_countWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个分区输出一次open和close
        env.setParallelism(4);
        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
//        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 开窗测试 计数窗口测试
        SingleOutputStreamOperator<Double> avgTempResult = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());
        avgTempResult.print();
        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> doubleIntegerTuple2) {
            return new Tuple2<Double, Integer>(doubleIntegerTuple2.f0 + sensorReading.getTemperature(),doubleIntegerTuple2.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0/doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
            return new Tuple2<>(doubleIntegerTuple2.f0+acc1.f0,doubleIntegerTuple2.f1+acc1.f1);
        }
    }
}
