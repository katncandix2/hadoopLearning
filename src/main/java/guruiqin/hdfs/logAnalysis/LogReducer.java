package guruiqin.hdfs.logAnalysis;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text,LogBean,Text, LogBean> {

    @Override
    protected void reduce(Text key, Iterable<LogBean> values, Context context) throws IOException, InterruptedException {

        long up_flow = 0;
        long d_flow = 0;

        for (LogBean log:values){
            up_flow += log.getUp_flow();
            d_flow  += log.getD_flow();
        }

        context.write(key,new LogBean(key.toString(),up_flow,d_flow));
    }
}
