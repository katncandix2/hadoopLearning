package guruiqin.hdfs.com;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UseHdfs {

    public static void main(String args[]) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS","hdfs://ruiqingu.com:9000/");

        FileSystem fs = FileSystem.get(new URI("hdfs://ruiqingu.com:9000/"),conf,"root");

        System.out.println(fs.getScheme());

        Path src = new Path("hdfs://ruiqingu.com:9000/data/hadoop.txt");

        FSDataInputStream in = fs.open(src);

        FileOutputStream out = new  FileOutputStream("/Users/guruiqin/data/hadoop.txt");

        IOUtils.copy(in,out);
    }

}
