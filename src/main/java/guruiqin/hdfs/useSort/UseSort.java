package guruiqin.hdfs.useSort;

import guruiqin.hdfs.logAnalysis.LogBean;
import guruiqin.hdfs.logAnalysis.LogMapper;
import guruiqin.hdfs.logAnalysis.LogReducer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UseSort {

    public static class SortMapper extends Mapper<LongWritable, Text, LogBean, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String []fileds = StringUtils.split(line,' ');

            String phoneNum = fileds[0];
            long up_flow    = Long.parseLong(fileds[1]);
            long d_flow    = Long.parseLong(fileds[2]);

            context.write(new LogBean(phoneNum,up_flow,d_flow),NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<LogBean,NullWritable,LogBean,NullWritable>{
        @Override
        protected void reduce(LogBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(UseSort.class);

        //当前job 使用的mapper 和 reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(LogBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reducer 输出 k-v 格式
        job.setOutputKeyClass(LogBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定input 文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }


}
