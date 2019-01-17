package guruiqin.hdfs.logAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LogRunner extends Configured implements Tool {

    public static void  main(String []args) throws Exception {

        ToolRunner.run(new Configuration(),new LogRunner(),args);

    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogRunner.class);

        //当前job 使用的mapper 和 reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //设置reducer 输出 k-v 格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LogBean.class);

        //设置mapper 输出 k-v 格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogBean.class);

        //指定input 文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true)?1:0;

    }
}
