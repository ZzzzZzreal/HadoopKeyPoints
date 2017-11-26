package flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FlowSum的Combiner类-------数据量小的时侯用不用没什么差别
 * 它继承Reducer，指定泛型<Text,FlowSumWritable,Text,FlowSumWritable>
 * Combiner的泛型K_in,V_in,K_out,V_out必须要和Mapper的K_out,V_out一致，也跟Reducer的K_in,V_in一致
 * Combiner是一个本地Reducer，只能起到过渡和优化的作用，它能做一些像归约类的对输出结果不造成影响的任务，比如求和
 *
 * 为什么会使用Combiner？---Reducer是在运行在网络环境上的，当数据量太大时，网络I/O速度慢，会导致效率低下。用本地的Reducer过渡，预处理可以提高效率
 */
public class FlowSumCombiner extends Reducer<Text,FlowSumWritable,Text,FlowSumWritable> {

    FlowSumWritable flowSumWritable = new FlowSumWritable();

    /**
     * 重写reduce方法
     * 因为Combiner是一个本地reducer，对Reducer进行优化，所以它的逻辑可以与Reducer完全相同，也可以不相同
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowSumWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //循环遍历
        for (FlowSumWritable flowSumWritable:values) {
            // sum+=(flowSumWritable.getUpFlow()+flowSumWritable.getDownFlow());
            sum+=flowSumWritable.getFlowSum();
            //打印日志
            System.out.println(" Combiner for sum:"+sum);
        }
        //打印日志
        System.out.println("Combiner sum"+sum);
        //将sum写入flowSumWritable
        flowSumWritable.setFlowSum(sum);
        //打印日志
        System.out.println("Combiner 写出的sum:"+flowSumWritable.getFlowSum());
        flowSumWritable.setPhone(key.toString());
        context.write(key,flowSumWritable);
    }
}
