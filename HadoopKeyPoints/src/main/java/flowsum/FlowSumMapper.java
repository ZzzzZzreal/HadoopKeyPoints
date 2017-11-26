package flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FlowSum的Mapper
 * 继承Mapper，指定泛型<LongWritable,Text,Text,FlowSumWritable>
 */
public class FlowSumMapper extends Mapper<LongWritable,Text,Text, FlowSumWritable> {

    String[] words;
    Text text = new Text();
    FlowSumWritable flowSumWritable = new FlowSumWritable();

    /**
     * 重写map方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //按制表符切割读取的内容
        words = value.toString().split("\t+");
        //手机号索引为1，将手机号存入text
        text.set(words[1]);
        //打印日志
        System.out.println("Mapper text:"+text);
        //将手机号、上行流量、下行流量存入flowSumWritable
        flowSumWritable.setPhone(words[1]);
        flowSumWritable.setFlowSum(Integer.parseInt(words[words.length-3])+ Integer.parseInt(words[words.length-2]));
        //打印日志
        System.out.println("sum"+flowSumWritable.getFlowSum());
        //写入环形缓存区
        context.write(text, flowSumWritable);

    }
}