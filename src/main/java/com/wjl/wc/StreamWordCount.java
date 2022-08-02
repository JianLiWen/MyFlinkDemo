package com.wjl.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Description:流处理
 * @Author: wenjianli
 * @Date: 2021/9/18 17:02
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        // 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        // 读取数据
//        String inputPath = "D:\\wenjianli3\\MyFlink\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);
        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);
        // 执行任务
        resultStream.print();
        env.execute();
    }
}
