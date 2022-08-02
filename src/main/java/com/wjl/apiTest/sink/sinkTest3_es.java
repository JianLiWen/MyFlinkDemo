package com.wjl.apiTest.sink;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class sinkTest3_es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //定义es的host配置
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsMapper()).build());
        env. execute();
    }

    public static class MyEsMapper implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入的数据datasource
            HashMap<String,String> dataSource = new HashMap<>();
            dataSource.put("id",sensorReading.getId());
            dataSource.put("temperature",sensorReading.getTemperature().toString());
            dataSource.put("time",sensorReading.getTimeStamp().toString());

            // 创建请求，作为向es发起的写入命令
            IndexRequest request = Requests.indexRequest().index("sensor").type("readingData").source(dataSource);

            // 发送请求
            requestIndexer.add(request);
         }

    }
}
