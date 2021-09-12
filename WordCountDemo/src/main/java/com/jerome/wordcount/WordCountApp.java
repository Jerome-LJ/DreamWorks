package com.jerome.wordcount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.jerome.wordcount.component.WordCountMapper;
import com.jerome.wordcount.component.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.io.compress.BZip2Codec;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.GzipCodec;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 打包 jar 到集群使用 hadoop 命令提交作业
 *
 * creat_date: 2021/09/03
 * creat_time: 上午10:30
 * 公众号：大数据梦工厂
 */

public class WordCountApp {

    // 1、使用硬编码，显示参数，实际开发中可以通过外部传参
    private static final String HDFS_URL = "hdfs://172.20.4.82:8020";
    private static final String HADOOP_USER_NAME = "hdfs";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 文件的输入路径和输出路径由外部传参指定
        if (args.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        // Configuration conf = new Configuration();
        // 开启 map 端输出压缩
        // conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置 map 端输出压缩方式
        // conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        // 指定 Hadoop 用户名，否则在 HDFS 上创建目录时可能会抛出权限不足的异常
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);

        Configuration configuration = new Configuration();
        // 指定 HDFS 的地址
        configuration.set("fs.defaultFS", HDFS_URL);

        // 2、获取 job 对象
        Job job = Job.getInstance(configuration);

        // 3、设置 jar 存储位置
        job.setJarByClass(WordCountApp.class);

        // 4、关联 Mapper 和 Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置 Reduce 个数
        //job.setNumReduceTasks(1);

        // 5、设置 Mapper 阶段输出数据的 key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 6、设置 Reducer 阶段输出数据的 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
        // job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置 4m
        // CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 虚拟存储切片最大值设置 20m
        // CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 设置 Reduce 端输出压缩开启
        // FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        // FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // 7、如果输出目录已经存在，则必须先删除，否则重复运行程序时会抛出异常
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER_NAME);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        // 8、设置输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 9、提交 job 到群集并等待它完成，参数设置为 true 代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);

        // 10、退出程序
        System.exit(result ? 0 : 1);
    }
}