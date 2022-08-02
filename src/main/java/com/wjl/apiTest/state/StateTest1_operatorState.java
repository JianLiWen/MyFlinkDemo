package com.wjl.apiTest.state;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;
import scala.Int;

import java.util.Collections;
import java.util.List;

/**
 * @author wenjianli
 * @date 2022/2/20 7:25 下午
 */
public class StateTest1_operatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
//        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前分区个数
        SingleOutputStreamOperator<Integer> result = dataStream.map(new MyCountMapper());
        result.print();
        env.execute();
    }

    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        // 定义一个本地变量作为算子状态
        public Integer count = 0;
        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        // 保证故障恢复
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num: state){
                count+=num;
            }
        }
    }
}
