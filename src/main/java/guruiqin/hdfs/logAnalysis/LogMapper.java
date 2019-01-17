package guruiqin.hdfs.logAnalysis;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text,Text,LogBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line    =  value.toString();
        String []datas = StringUtils.split(line," ");

        String phoneNum = datas[0];
        Long   up_flow  = Long.parseLong(datas[1]);
        Long   d_flow   = Long.parseLong(datas[2]);

        context.write(new Text(phoneNum),new LogBean(phoneNum,up_flow,d_flow));
    }
}
