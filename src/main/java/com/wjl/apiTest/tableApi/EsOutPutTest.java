package com.wjl.apiTest.tableApi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import scala.util.parsing.json.JSON;

/**
 * @author wenjianli
 * @date 2022/7/26 7:39 下午
 */
public class EsOutPutTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new Kafka().version("0.11").topic("sensor")
                .property("zookeeper.connect","localhost:2181")
                .property("bootstrap.servers","localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id,temp").filter("id === 'sensor_6'");
        Table aggTable = sensorTable.groupBy("id").select("id,id.count as count");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        // 输出到ES
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("localhost",9200,"http")
                .index("sensor")
                .documentType("temp"))
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema().field("id",DataTypes.STRING()).field("count",DataTypes.BIGINT()))
                .createTemporaryTable("outPutTable");
        aggTable.insertInto("outPutTable");
//        Table outPutTable = tableEnv.from("outPutTable");
//        tableEnv.toAppendStream(outPutTable,Row.class).print("out");
        env.execute();
    }
}
