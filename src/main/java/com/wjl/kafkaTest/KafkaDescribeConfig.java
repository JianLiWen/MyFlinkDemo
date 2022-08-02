package com.wjl.kafkaTest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wenjianli
 * @date 2022/3/17 8:10 下午
 */
public class KafkaDescribeConfig {
    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String topic = "topic-admin";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,topic);
        // 连接kafka集群
        AdminClient client = AdminClient.create(properties);
        // 创建主题
        NewTopic newTopic = new NewTopic(topic,4,(short)1);
        // 指定需要覆盖的配置
        Map<String,String> config = new HashMap<>();
        config.put("cleanup.policy","compact");
        newTopic.configs(config);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try{
            result.all().get();
        }catch (Exception e){
            e.printStackTrace();
        }
        client.close();
    }
}
