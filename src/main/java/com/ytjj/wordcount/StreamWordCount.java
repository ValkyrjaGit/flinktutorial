package com.ytjj.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
//        //从文件中读取数据
//        String inputpath = "D:\\Other\\flinktutorial\\src\\main\\resources\\hello.txt";
//
//        DataStreamSource<String> inputDataStream = env.readTextFile(inputpath);

        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);

        //基于数据流进行转换计算

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyflatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();
        //执行任务
        env.execute();

    }
}
