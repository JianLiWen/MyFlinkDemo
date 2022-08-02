package com.wjl.apiTest.state;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

import java.util.Collections;
import java.util.List;

/**
 * @author wenjianli
 * @date 2022/2/20 7:25 下午
 */
public class StateTest1_keyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
//        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> result = dataStream.keyBy("id")
                .map(new MyKeyCountMapper());
        result.print();
        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer>  keyCountState ;

        // 其他类型状态的声明
        private ListState<String> myListState;

        private MapState<String,Double> mapState;

        private ReducingState<SensorReading> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class));

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list",String.class));

            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("map-list",String.class,Double.class));

//            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>());
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Iterable<String> states = myListState.get();
            for (String state:states){
                System.out.println(state);
            }
            myListState.add("hello");

            mapState.get("1");
            mapState.put("2",32.5);
            // reducing state
            reducingState.add(sensorReading);
            reducingState.clear();

            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
