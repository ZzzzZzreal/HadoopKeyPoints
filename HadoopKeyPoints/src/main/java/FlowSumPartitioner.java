import flowsum.FlowSumWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * 业业需求：按照手机号段134、135、136、138划分地域
 *
 * 业务分析：所谓的划分区域，就是按照手机号段分区，将读取到的数据按不同号段写入到不同的文件中
 */
public class FlowSumPartitioner {



    //自定义分区类
    /**
     * 静态内部类---Partitioner
     */
    static class FlowSumPart extends Partitioner<Text,FlowSumWritable> {
        /**
         * 定义一个静态的Map集合，K值是需要分区的号段，V值是分区（也可以理解为要写入的文件）编号
         */
        static HashMap<String,Integer> cc = new HashMap<String,Integer>();
        //在静态代码块中对集合赋初始值
        static {
            cc.put("134",0);
            cc.put("135",1);
            cc.put("136",2);
            cc.put("138",3);
        }
        //将m定义成引用类型，方便从集合中获取（value）
        Integer m;

        String key;
        public int getPartition(Text text,FlowSumWritable flowSumWritable, int numPartitions) {
           //key为读取到的一行的内容
            key = text.toString();
//            if (key.startsWith("133")){
//                numPartitions = 0;
//            }
//            else if (key.startsWith("166")){
//                numPartitions = 1;
//            }
//            else if (key.startsWith("188")){
//                numPartitions = 2;
//            }
//            else if (key.startsWith("144")){
//                numPartitions = 3;
//            }else{
//                numPartitions = 4;
//            }
            /**
             * 以上代码实现起来更为方便，而且逻辑更简单，但为了更好的装逼，使用一个逼格稍高的集合来实现相同的逻辑
             */
            //截取这一行内容的前三个字符
            key = key.substring(0,3);
            //打印日志
            System.out.println(key+"---->");
            //获取集合中对应key的value值
            m = cc.get(key);
            //打印日志
            System.out.println(key+"---->");
            //如果m为空，返回4；如果不为空，返回对应的m值---这里还应用到Integer的自动拆包
            return m==null?4:m;
        }
    }

    /**
     * 静态内部类---Mapper
     */
    static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowSumWritable> {

        String[] words;
        Text text = new Text();
        FlowSumWritable flowSumWritable = new FlowSumWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //按制表符切割读取的内容
            words = value.toString().split("\t+");
            //手机号索引为1，将手机号存入text
            text.set(words[1]);
            //将手机号、总流量存入flowSumWritable
            flowSumWritable.setPhone(words[1]);
            flowSumWritable.setFlowSum(Integer.parseInt(words[words.length - 3]) + Integer.parseInt(words[words.length - 2]));
            //写入环形缓存区
            context.write(text, flowSumWritable);

        }
    }

    /**
     * 静态内部类---Reducer
     */
    static class FlowSumReducder extends Reducer<Text, FlowSumWritable, Text, IntWritable> {

        IntWritable intWritable = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<FlowSumWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            //循环遍历
            for (FlowSumWritable flowSumWritable : values) {
                sum += flowSumWritable.getFlowSum();
            }
            //将sum写入intWritable
            intWritable.set(sum);
            context.write(key, intWritable);
        }
    }

    public static void main(String[] agrs)throws Exception {

        //创建一个Configuration实例
        Configuration conf = new Configuration();
        //根据conf来获得一个Job的实例，通过这个Job来实现任务的设置
        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(FlowSumPartitioner.class);
        //设置Mapper类
        job.setMapperClass(FlowSumMapper.class);
        //设置Reducer类
        job.setReducerClass(FlowSumReducder.class);
        //设置分区类
        job.setPartitionerClass(FlowSumPart.class);

        //设置输出类型---如果Mapper和Reducer的输出类型相同，可以只设置outputKey和outputValue；如果不同则需要设置outputKey和outputValue、MapoutputKey和MapoutputValue
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowSumWritable.class);

        //设置你的reduceTask数量，有几个就会生出几个文件
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job,new Path("DATA/mobileflow.log"));
        Path path = new Path("RESULT/partitioner");
        //删除你的输出文件夹
        path.getFileSystem(conf).delete(path,true);

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);


    }
}