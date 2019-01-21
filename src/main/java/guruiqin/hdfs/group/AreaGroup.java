package guruiqin.hdfs.group;

import guruiqin.hdfs.logAnalysis.LogBean;
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
 * 1. 继承Partitioner  重写getPartition 方法
 * 2. 设置reduce 任务数量
 */
public class AreaGroup {

    public static class AreaMapper extends Mapper<LongWritable, Text, Text, LogBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String []data = StringUtils.split(line,' ');

            long up_flow = Long.parseLong(data[1]);
            long d_flow  = Long.parseLong(data[2]);

            context.write(new Text(data[0]),new LogBean(data[0],up_flow,d_flow));
        }
    }

    public static class AreaReducer extends Reducer<Text,LogBean,Text,LogBean>{

        @Override
        protected void reduce(Text key, Iterable<LogBean> values, Context context) throws IOException, InterruptedException {

            long up_flow = 0;
            long d_flow  = 0;
            for (LogBean bean:values){
                up_flow += bean.getUp_flow();
                d_flow  += bean.getD_flow();
            }

            context.write(key,new LogBean(key.toString(),up_flow,d_flow));
        }
    }

    public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //设置对应jar
        job.setJarByClass(AreaGroup.class);


        //设置自定义分组逻辑
        job.setPartitionerClass(AreaPartition.class);

        //设置reduce 任务数
        job.setNumReduceTasks(3);

        //当前job 使用的mapper 和 reducer
        job.setMapperClass(AreaMapper.class);
        job.setReducerClass(AreaReducer.class);

        //设置mapper 输出key value 格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogBean.class);

        //设置reducer 输出 k-v 格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LogBean.class);

        //指定input 文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
