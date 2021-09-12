package com.jerome.wordcount.component;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 将每行数据按照分隔符(" \t\n\r\f")进行拆分

 * Object      : Mapping 输入文件的内容
 * Text        : Mapping 输入的每一行的数据
 * Text        : Mapping 输出 key 的类型
 * IntWritable : Mapping 输出 value 的类型
 *
 * creat_date: 2021/09/03
 * creat_time: 上午10:30
 * 公众号：大数据梦工厂
 */

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 获取到一行文件的内容
        String line = value.toString();
        // 按照空格切分这一行的内容为一个单词数组
        String[] words = StringUtils.split(line, " ");
        // 遍历输出 <key, value> 键值对
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
