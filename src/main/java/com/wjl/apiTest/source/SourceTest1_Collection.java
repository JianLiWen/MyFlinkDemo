package com.wjl.apiTest.source;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度为1
        env.setParallelism(1);
        // 1.从集合读取数据
        DataStream<SensorReading> sensorDataStream = env.fromCollection(Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)));
        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3, 4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("tempTable",integerDataStream);
        Table table = tableEnv.fromDataStream(sensorDataStream);
//        tableEnv.createTemporaryView("table",table);
        String explain = tableEnv.explain(table);
        System.out.println(explain);
        // 2.打印输出
        sensorDataStream.print("data");
        integerDataStream.print("int");
        // 两个同时执行不存在依赖关系，所以这两个数据流应该是并行执行的，内部数据流串行
        // 3.执行
        env.execute();
    }
}
