package com.wjl.kafkaTest;

import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author wenjianli
 * @date 2022/3/9 8:44 下午
 */
public class FirstMultiConsumerThreadDemo {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic_demo";
    public static final String groupId = "group.demo";

    public static Properties initConfig(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        return properties;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        int consumerThreadNum =4;
        for (int i=0;i<consumerThreadNum;i++){
            new KafkaConsumerThread(props,topic);
        }
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String,String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic){
            this.kafkaConsumer = new KafkaConsumer<>(props);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while (true){
                    ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String,String> record:records){

                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                kafkaConsumer.close();
            }
        }
    }
}
