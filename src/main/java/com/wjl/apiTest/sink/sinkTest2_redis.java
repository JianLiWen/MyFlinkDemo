package com.wjl.apiTest.sink;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class sinkTest2_redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个分区输出一次open和close
        DataStream<String> inputStream = env.readTextFile("/Users/wenjianli3/IdeaProjects/MyFlink/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        dataStream.addSink(new RedisSink<SensorReading>(config,new MyRedisMapper()));
        env. execute();
    }

    // 自定义redisMapper
    public static class MyRedisMapper  implements RedisMapper<SensorReading>{

        // 定义保存数据到redis的命令，存成hash表 hset
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemperature().toString();
        }
    }
}
