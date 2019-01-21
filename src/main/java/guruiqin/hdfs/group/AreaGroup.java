package guruiqin.hdfs.group;

import guruiqin.hdfs.logAnalysis.LogBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AreaGroup {

    public static class AreaMapper extends Mapper<LongWritable, Text, Text, LogBean> {
        
    }
}
