package com.wjl.apiTest.sink;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class sinkTest4_jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //定义es的host配置
        List<HttpHost> httpHosts = new ArrayList<>();
        dataStream.addSink(new MyJdbcSink());
        env. execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;


        // 创建连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");
            insertStmt = conn.prepareStatement("insert into sensor_temp(id,temp) values (?,?) ");
            updateStmt = conn.prepareStatement("update sensor_temp set temp =? where id = ?");
        }

        // 调用连接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount()==0){
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
