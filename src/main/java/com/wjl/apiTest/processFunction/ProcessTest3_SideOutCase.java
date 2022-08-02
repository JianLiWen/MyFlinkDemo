package com.wjl.apiTest.processFunction;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wenjianli
 * @date 2022/2/27 6:13 下午
 */
public class ProcessTest3_SideOutCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个OutPutTag，用来表示侧输出流 低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){
        };
        // 测试 ,自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
               //判断温度大于30高温流输出到主流，小于30输出侧输出流
                if (value.getTemperature() > 30){
                    out.collect(value);
                }else {
                    ctx.output(lowTempTag,value);
                }
            }
        });
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");
        env.execute();
    }

}
