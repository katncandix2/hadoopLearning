package guruiqin.hdfs.rpc;

public class LoginImpl implements Login {

    public String login(String userName, String passWord) {
        return "hello"+userName;
    }
}
