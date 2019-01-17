package guruiqin.hdfs.rpc;

public interface Login {

    public static final long versionID = 1L;

    public String login(String userName,String passWord);
}
