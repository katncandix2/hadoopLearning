package guruiqin.hdfs.useSort;

import guruiqin.hdfs.logAnalysis.LogBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UseSort {

    public static class SortMapper extends Mapper<LongWritable, Text, LogBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = key.toString();

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

    public static void main(String []args){

    }


}
