package com.wjl.kafkaTest;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author wenjianli
 * @date 2022/3/17 8:10 下午
 */
public class KafkaCreateTopics {
    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String topic = "apple";
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

    public static void describeTopicConfig() throws ExecutionException, InterruptedException {
        String brokerList = "localhost:9092";
        String topic = "topic-admin";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,topic);
        // 连接kafka集群
        AdminClient client = AdminClient.create(properties);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC,topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Config config = result.all().get().get(resource);
        System.out.println(config);
        client.close();
    }

    public static void alterTopicConfig() throws ExecutionException, InterruptedException {
        String brokerList = "localhost:9092";
        String topic = "topic-admin";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,topic);
        // 连接kafka集群
        AdminClient client = AdminClient.create(properties);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC,topic);
        ConfigEntry entry  = new ConfigEntry("cleanup.policy","compact");
        Config config = new Config(Collections.singleton(entry));
        Map<ConfigResource,Config> configs = new HashMap<>();
        configs.put(resource,config);
        AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();
        client.close();
    }

    public static void addPartitions() throws ExecutionException, InterruptedException {
        String brokerList = "localhost:9092";
        String topic = "topic-admin";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,topic);
        // 连接kafka集群
        AdminClient client = AdminClient.create(properties);
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String,NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic,newPartitions);
        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        result.all().get();
    }

}
