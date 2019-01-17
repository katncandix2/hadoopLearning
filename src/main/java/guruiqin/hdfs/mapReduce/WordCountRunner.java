package guruiqin.hdfs.mapReduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountRunner {

    public static void  main(String []args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountRunner.class);

        //当前job 使用的mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置reducer 输出 k-v 格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置mapper 输出 k-v 格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定input 文件路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://ruiqingu.com:9000/wc/srcdata/"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://ruiqingu.com:9000/wc/output1/"));


        job.waitForCompletion(true);

    }
}
