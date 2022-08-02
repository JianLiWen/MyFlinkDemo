package com.wjl.apiTest.window;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.util.Collector;

public class windowTest1_timeWindow {
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
        // 开窗测试 增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
//                .countWindow(3,2)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                .timeWindow(Time.seconds(15))
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                })
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer+1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1 ;
                    }
                });

        // 全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> resultStream2 = dataStream.keyBy("id").timeWindow(Time.seconds(15)).apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                String id = tuple.getField(0);
                Long windowEnd = window.getEnd();
                Integer count = IteratorUtils.toList(input.iterator()).size();
                out.collect(new Tuple3<>(id,windowEnd,count));
            }
        });
        resultStream2.print();
        env.execute();
    }
}
