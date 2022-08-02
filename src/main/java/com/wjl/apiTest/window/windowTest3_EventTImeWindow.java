package com.wjl.apiTest.window;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author wenjianli
 * @date 2022/1/22 7:24 下午
 */
public class windowTest3_EventTImeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 周期性生成watermark
        env.getConfig().setAutoWatermarkInterval(100);
        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
        // 设置乱序数据事件事件戳和watermark
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimeStamp() * 1000L;
            }
        });
                // 升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimeStamp() * 1000L;
//                    }
//                })
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){

        };
        // 基于事件时间的开窗集合,统计15秒内的温度最小值
        SingleOutputStreamOperator<SensorReading> minStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");
        minStream.print("minTemp");
        minStream.getSideOutput(outputTag).print("late");
        env.execute();

    }
}
