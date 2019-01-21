package guruiqin.hdfs.group;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

//map-reduce 使用此方法进行分组
public class AreaPartition<KEY,VALUE> extends Partitioner <KEY,VALUE>{

    private  static HashMap<String,Integer> nameHash = new HashMap<String, Integer>();

    static {
        nameHash.put("guruiqin",1);
        nameHash.put("liuyanan",2);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int i) {

        int areaCode  = nameHash.get(key.toString())==null?1:nameHash.get(key.toString());
        return areaCode;
    }
}
