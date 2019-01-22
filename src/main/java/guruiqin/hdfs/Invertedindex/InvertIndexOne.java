package guruiqin.hdfs.Invertedindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 倒排索引实现
 * 步骤1 根据 词+文件名 输出 [word]+[fileName] --> [word-count]
 */
public class InvertIndexOne {

    public static class StepOneMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String []data = StringUtils.split(line,' ');

            //获取文件名
            FileSplit fileSplit =(FileSplit) context.getInputSplit();

            String fileName = fileSplit.getPath().getName();

            for (String s:data){

                //输出为  word-->xx.txt 1
                context.write(new Text(s+"-->"+fileName),new LongWritable(1));
            }
        }
    }

    public static class StepOneReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;

            for (LongWritable v:values){
                count+=v.get();
            }

            context.write(key,new LongWritable(count));
        }
    }

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(InvertIndexOne.class);

        //当前job 使用的mapper 和 reducer
        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);

        //设置reducer 输出 k-v 格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置mapper 输出 k-v 格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定input 文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
