package com.wjl.apiTest.tableApi;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wenjianli
 * @date 2022/7/31 11:25 上午
 */
public class TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2.读入文件数据得到DataStream
        DataStreamSource<String> inputStream = env.readTextFile("/Users/wenjianli/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        // 3.转换POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimeStamp()*1000L;
            }
        });
        // 4.将流转换成表，定义时间特性
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timeStamp as ts,temperature as temp,pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timeStamp as ts,temperature as temp,rt.rowtime");
//        tableEnv.toAppendStream(dataTable, Row.class).print();
        dataTable.printSchema();
        tableEnv.createTemporaryView("sensor",dataTable);
        // 5.窗口操作
        // 5.1group window
        // tableAPi
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");
        // Sql
        Table resultSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt,interval '10' second)" + " from sensor group by id, tumble(rt,interval '10' second) ");
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.toRetractStream(resultSqlTable,Row.class).print("sql");
        // 5.2 overWindow
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temp.avg over ow");
        // sql
        Table overSqlResult = tableEnv.sqlQuery("select id,rt,count(id) over ow,avg(temp) over ow" +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");
        tableEnv.toAppendStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult,Row.class).print("overSqlResult");
        env.execute();
    }
}
