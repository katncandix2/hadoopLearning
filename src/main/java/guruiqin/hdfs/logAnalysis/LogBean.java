package guruiqin.hdfs.logAnalysis;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogBean implements WritableComparable<LogBean> {

    private String phoneNum;
    private long up_flow;
    private long d_flow;

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }

    private long s_flow;


    public LogBean(String phoneNum, long up_flow, long d_flow) {
        this.phoneNum = phoneNum;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.s_flow = up_flow+d_flow;
    }

    public LogBean(){}

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeLong(up_flow);
        out.writeLong(d_flow);
        out.writeLong(s_flow);
    }

    public void readFields(DataInput in) throws IOException {
        phoneNum = in.readUTF();
        up_flow  = in.readLong();
        d_flow   = in.readLong();
        s_flow   = in.readLong();
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "phoneNum='" + phoneNum + '\'' +
                ", up_flow=" + up_flow +
                ", d_flow=" + d_flow +
                ", s_flow=" + s_flow +
                '}';
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public int compareTo(LogBean o) {
        return  s_flow>o.s_flow?-1:1;
    }
}
