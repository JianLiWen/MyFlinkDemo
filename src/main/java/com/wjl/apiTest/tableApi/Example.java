package com.wjl.apiTest.tableApi;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wenjianli
 * @date 2022/6/1 9:43 上午
 */
public class Example {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("/Users/wenjianli/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        // 2.转换POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 3.创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 4. 基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);
        // 5. 调用table API进行转换操作
        Table resultTable = dataTable.select("id, temperature").where("id = 'sensor_6'");

        // 6. 执行sql
        tableEnv.createTemporaryView("sensor",dataTable);
        String sql = "select id,temperature from sensor where id = 'sensor_6'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }
}
