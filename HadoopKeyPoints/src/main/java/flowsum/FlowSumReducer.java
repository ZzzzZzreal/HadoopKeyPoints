package flowsum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FlowSum的Reducer
 * 继承Reducer，指定泛型<Text,FlowSumWritable,Text,IntWritable>
 */
public class FlowSumReducer extends Reducer<Text,FlowSumWritable,Text,IntWritable>{

    IntWritable intWritable = new IntWritable();

    /**
     * 重写reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowSumWritable> values, Context context) throws IOException, InterruptedException {

        //打印日志
        System.out.println("Reducer key:"+key);
        int sum = 0;
        //循环遍历
        for (FlowSumWritable flowSumWritable:values) {
           // sum+=(flowSumWritable.getUpFlow()+flowSumWritable.getDownFlow());
            sum+=flowSumWritable.getFlowSum();
            //打印日志
            System.out.println("for sum:"+sum);
        }
        //打印日志
        System.out.println("reducer sum"+sum);
        //将sum写入intWritable
        intWritable.set(sum);
        context.write(key,intWritable);
    }
}
