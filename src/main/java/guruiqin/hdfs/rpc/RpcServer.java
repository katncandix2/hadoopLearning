package guruiqin.hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;

import java.io.IOException;

public class RpcServer {

    public static void main(String []args) throws IOException {

        Builder builder = new RPC.Builder(new Configuration());

        builder.setBindAddress("localhost")
                .setPort(12345)
                .setProtocol(Login.class)
                .setInstance(new LoginImpl());

        RPC.Server server = builder.build();

        server.start();
    }

}
