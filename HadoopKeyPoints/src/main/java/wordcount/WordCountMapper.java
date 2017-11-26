package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WordCount的Mapper类
 * 需要继承Mapper，并指定泛型<K_in,V_in,K_out,V_out>
 * 业务需求：统计一个文件中每个单词出现的次数，并且单词中不能有：，.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    String[] words;//定义一个字符串数组，用于存储切割后的字符串
    Text text = new Text();//创建一个Text实例---这里的Text是hadoop中的数据类型，相当于java中的string
    //创建一个值为1的LongWritable实例，设置它的值为1为了方便后面Reducer计数---这里的LongWritable也是hadoop中的数据类型，相当于java中的long
    LongWritable longWritable = new LongWritable(1);
    /**
     * 重写map方法。
     * @param key------------Mapper的key_in,用不到
     * @param value----------Mapper的value_in,传入的需要进行处理的数据
     * @param context--------上下文，可以理解为map阶段中的环形缓存区
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*String[] words;
        Text text = new Text();
        LongWritable longWritable = new LongWritable(1);*/
        /**
         * 以上代码在执行上毫无问题，但考虑到Mapper的工作模式--- “对来的每条数据进行一次计算（这里的计算指的是执行一遍代码逻辑）“
         * 如果在重写的map方法中new对象，会导致浪费大量的内存资源，如果数据量太大甚至会导致内存崩溃
         * 所以，正确的思路是在自己的Mapper类中创建实例，每次调用map()方法时使用实例，优化内存及代码效率
         */

        //按照业务需求将出现的 ， . ： 去除，并将文件内容按空格切割
        words = (((value.toString().replaceAll(","," ")).replaceAll("\\."," "))
                .replaceAll(":"," ")).split(" ");
       //循环遍历，得到每个单词
        for (String word:words) {
            text.set(word);//将切割出来的每个单词放入text中
            context.write(text,longWritable);//将text和longWritable写入context中
        }
    }
}
