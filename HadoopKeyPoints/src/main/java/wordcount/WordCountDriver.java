package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * WordCount的Driver
 * Driver中有main方法，是程序的入口
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {

        //创建一个Configuration实例
        Configuration conf = new Configuration();
        //根据conf来获得一个Job的实例，通过这个Job来实现任务的设置
        Job job = Job.getInstance(conf);

        //设置主类---有main方法的类，即Driver
        job.setJarByClass(WordCountDriver.class);
        //设置Mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置Reducer类
        job.setReducerClass(WordCountReducer.class);

        //设置输出类型---如果Mapper和Reducer的输出类型相同，可以只设置outputKey和outputValue；如果不同则需要设置outputKey和outputValue、MapoutputKey和MapoutputValue
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path("DATA/wc.txt"));
        Path path = new Path("RESULT/wordcount");
        path.getFileSystem(conf).delete(path,true);//删除你的输出文件夹
        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
