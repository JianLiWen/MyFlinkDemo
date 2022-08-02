package com.wjl.apiTest.source;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<SensorReading> dataStream = env.addSource(new MySensorDataSource());
            dataStream.print();
            env.execute();
    }

    // 实现自定义的sourceFunction
    public static class MySensorDataSource implements SourceFunction<SensorReading>{
        //定义一个标识位，控制数据的产生
        private boolean running = true;
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置10个传感器的初始温度
            HashMap<String,Double> sensorMap = new HashMap<String, Double>();
            for (int i=0;i<10;i++){
                sensorMap.put("sensor_" + (i+1), 60+ random.nextGaussian()*20);
            }

            while (running){
                for (String sensorId:sensorMap.keySet()){
                    //在当前温度基础上随机波动
                    Double nextTemp = sensorMap.get(sensorId) + random.nextGaussian();
                    sensorMap.put(sensorId,nextTemp);
                    sourceContext.collect(new SensorReading(sensorId,System.currentTimeMillis(),nextTemp));
                }
                // 控制输出频率
                Thread.sleep(1000L);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
