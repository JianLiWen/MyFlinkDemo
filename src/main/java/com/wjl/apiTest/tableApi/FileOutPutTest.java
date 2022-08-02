package com.wjl.apiTest.tableApi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author wenjianli
 * @date 2022/7/23 3:56 下午
 */
public class FileOutPutTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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
        // 4.输出到文件
        // 连接外部文件注册输出表
        String outPutPath = "/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/out2.txt";
        tableEnv.connect(new FileSystem().path(outPutPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id",DataTypes.STRING())
                        .field("count",DataTypes.BIGINT())
                .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("outPutTable");
//        resultTable.insertInto("outPutTable");
        aggTable.insertInto("outPutTable");
        env.execute();
    }
}
