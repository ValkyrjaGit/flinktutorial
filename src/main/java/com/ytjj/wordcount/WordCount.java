package com.ytjj.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//批处理
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String inputpath = "D:\\Other\\flinktutorial\\src\\main\\resources\\hello.txt";

        DataSet<String> inputDataSet = env.readTextFile(inputpath);
        //对数据集进行处理，按照空格分词展开，转换成（word，1）进行统计
        DataSet<Tuple2<String,Integer>> resultSet=inputDataSet.flatMap(new MyflatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和

        resultSet.print();
    }
    //自定义类，实现FlatMapFunction

    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = value.split(" ");
            //遍历所有word，包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }


        }
    }

}
