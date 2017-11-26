package flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义序列化类
 * 在这里需要对设置为key的子段进行序列化反序列化，即重写compareTo方法
 * 但在这次的业务中没有将自定义序列化类放在key的位置，所以没有重写compareTo方法
 */
public class FlowSumWritable implements Writable {

    String phone;
    int flowSum;

    /*//需要把手机号当成key，所以必须把无参构造写出来---测试证明不是必要的
    public FlowSumWritable() {
    }*/

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getFlowSum() {
        return flowSum;
    }

    public void setFlowSum(int flowSum) {
        this.flowSum = flowSum;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(phone);
        dataOutput.writeInt(flowSum);
    }

    public void readFields(DataInput dataInput) throws IOException {

        phone = dataInput.readUTF();
        flowSum = dataInput.readInt();
    }

    @Override
    public String toString() {
        return phone + " " + flowSum;
    }

}
