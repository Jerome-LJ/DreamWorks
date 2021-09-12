package com.jerome.wordcount.component;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 在 Reduce 中进行单词出现次数的统计

 * Text         : Mapping 输入的 key 的类型
 * IntWritable  : Mapping 输入的 value 的类型
 * Text         : Reducing 输出的 key 的类型
 * IntWritable  : Reducing 输出的 value 的类型
 *
 * creat_date: 2021/09/03
 * creat_time: 上午10:30
 * 公众号：大数据梦工厂
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 统计数字
        int count = 0;
        // 累加求和
        for (IntWritable value : values) {
            count += value.get();
        }
        // 输出 <单词：count> 键值对
        context.write(key, new IntWritable(count));
    }
}
