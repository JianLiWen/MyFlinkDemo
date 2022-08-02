package com.wjl.apiTest.processFunction;

import com.wjl.apiTest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * @author wenjianli
 * @date 2022/2/27 6:13 下午
 */
public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> inputStream = env.socketTextStream("127.0.0.1", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试，先分组然后自定义处理
        dataStream.keyBy("id").process(new TempConsIncreWarning(10)).print();

        env.execute();
    }

    // 检测一段时间内的温度连续上升，输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple,SensorReading,String>{
        // 定义私有属性，当前统计的时间间隔
        private Integer interval;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }
        // 定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            // 如果温度上升并且没有定时器，注册10s后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs ==null){
                // 计算定时器时间戳
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            // 如果温度下降，删除定时器
            else if (value.getTemperature()< lastTemp && timerTs!=null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            System.out.println("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
