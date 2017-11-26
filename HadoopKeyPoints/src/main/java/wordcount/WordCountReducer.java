package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * WordCount的Reducer类
 * 需要继承Reducer类，并指定泛型<K_in,V_in,K_out,V_out>,Reducer的K_in和V_in的数据类型必须和Mapper的K_out,V_out一致
 */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

    //创建一个LongWritable实例
    LongWritable longWritable = new LongWritable();

    /**
     *重写reduce方法
     *
     * @param key-------------------Reducer的K_in,也就是Mapper输出的K_out,实际从Mapper传出的K_out是切割的每个文档的单词
     * @param values----------------Reducer的V_in,就是从Mapper输出的V_out,实际从Mapper传出的V_out是LongWritable类型的数据 1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        //定义一个long类型的变量sum用来计数
        long sum = 0;
        //接收的V_in是一个迭代器类型的变量，是一个集合，需要遍历
        for (LongWritable val:values) {
            /**
             *  计数器等于每个val相加
             *  因为每个val都是1，实际完成了每出现一次这个单词计数器加一
             *  而Reducer的工作模式是对同一个Key执行一次reduce，所以能实现统计每个单词出现的次数的功能
             */
            sum +=val.get();
        }
        //将统计完的计数器赋值到longWritable
        longWritable.set(sum);
        //将每个单词和出现的次数写入context
        context.write(key,longWritable);
    }
}
