package com.wjl.apiTest.tableApi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author wenjianli
 * @date 2022/6/1 5:26 下午
 */
public class CommonApi {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于老版本Planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment oldTableEnv = StreamTableEnvironment.create(env,oldStreamSettings);

        // 基于老版本Planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 基于blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env,blinkStreamSettings);

        // 基于blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        // 2.表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath =  "/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print();

        //  3.查询转换
        // 3.1 Table API简单转换
        Table resultTable = inputTable.select("id,temp").filter("id === 'sensor_6'");
        // 聚合
        Table aggTable = inputTable.groupBy("id").select("id,id.count as count,temp.avg as avgTemp");
        // 3.2 sql
        tableEnv.sqlQuery("select id,temp from inputTable where id = 'sensor_6'");
        Table sqlAgrTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");
        // 打印输出
        tableEnv.toAppendStream(resultTable,Row.class).print("result");
        tableEnv.toRetractStream(aggTable,Row.class).print("agg");
        tableEnv.toRetractStream(sqlAgrTable,Row.class).print("sqlAgg");

        env.execute();

    }

}
