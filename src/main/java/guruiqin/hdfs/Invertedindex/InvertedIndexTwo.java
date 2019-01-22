package guruiqin.hdfs.Invertedindex;

import jdk.internal.jline.internal.TestAccessible;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 倒排索引
 * 步骤2
 * 将得到的  [word]+[fileName] -> [word-count] ...
 * 进行汇总  得到 [word] + {[fileName1],[fileName2]...}
 */
public class InvertedIndexTwo {

    public static class StepTwoMapper extends Mapper<LongWritable, Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String []data = StringUtils.split(line,"-->");

            context.write(new Text(data[0]),new Text(data[1]));
        }
    }

    public static class StepTwoReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String line = "";

            for (Text s:values){
                line+= s.toString()+"\t";
            }

            context.write(key,new Text(line));

        }
    }

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(InvertedIndexTwo.class);

        //当前job 使用的mapper 和 reducer
        job.setMapperClass(StepTwoMapper.class);
        job.setReducerClass(StepTwoReducer.class);

        //设置reducer 输出 k-v 格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置mapper 输出 k-v 格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定input 文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
