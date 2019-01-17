package guruiqin.hdfs.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UpLoad {

//    static  final  String URL = "hdfs://ruiqingu.com:9000/";
    static  final  String URL = "hdfs://zhangliangfang.com:9000/";

    public static void main(String []args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS",URL);

        FileSystem fs = FileSystem.get(new URI(URL),conf,"root");

        mkdir(fs);
//        upload(fs);
        fs.close();
    }

    public static void mkdir(FileSystem fs) throws IOException {
        System.out.println("mkdir");
        fs.mkdirs(new Path("/test/hadoop"));
    }

    public static void upload(FileSystem fs) throws IOException {
        System.out.println("upload");
        fs.copyFromLocalFile(new Path("/Users/guruiqin/data/hadoop.txt"),new Path(URL));


    }

}
