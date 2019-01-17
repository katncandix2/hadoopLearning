package guruiqin.hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RpcClient {

    public static void main(String []args) throws IOException {

        Login proxy = RPC.getProxy(Login.class,1L,new InetSocketAddress("localhost",12345),new Configuration());

        String res = proxy.login("guruiqin","1234");

        System.out.println(res);
    }
}
