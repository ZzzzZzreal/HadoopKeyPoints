import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * 自定义输入格式----读两行数据
 * <p>
 * 思路：LineReader可以完成一行一行读的操作，所以可以每次读一行，读两次，然后写出
 * <p>
 * 数据流程是block-->Job--->InputFormat--->RecordReader-->Map
 */
public class MyFileInputFormat {

    /**
     * 自定义输入格式。需要继承FileInputFormat。
     * 里面主要是重写createRecordReader方法，返回一个自定义的RecordReader
     * <p>
     * 实际上还有一个重写方法是isSplitable，但是我们一般不作重写。因为hadoop不适合管理小文件，所以我们需要这个方法的返回值一直时true
     * 在InputFormat的源码中， 方法是这样的：protected boolean isSplitable(JobContext context, Path filename) {return true;}
     * 所以我们不需要重写这个方法
     */
    static class MyInput extends FileInputFormat<LongWritable, Text> {

        /*@Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return super.isSplitable(context, filename);
        }*/

        public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new RRer();
        }
    }

    /**
     * 自定义RecordReader，它是主要的处理输入数据格式的类，最终是写初始化方法initialize和nextKeyValue
     * 在自定义RecordReader中，需要重写的方法有：
     * initialize()---初始化方法，完成自定义字段的初始化
     * nextKeyValue()---在这个方法中写逻辑，对K和V进行赋值
     * getCurrentKey()---获取当前key
     * getCurrentValue()---获取当前value
     * getProgress()---获取进度，一般return true?0.0f:1.0f;
     * close()---如果开了流必须要关闭，如果没开流则不需要
     */
    static class RRer extends RecordReader<NullWritable, Text> {

        int len;
        LineReader lineReader;
        Configuration conf;
        FSDataInputStream fsDataInputStream;
        Text text = new Text();
        StringBuffer sb = new StringBuffer();
        Text textx = new Text();

        /**
         * 初始化方法---initialize
         * 思路：因为LineReader可以完成一行一行读的目的，所以初始化时，要做的事情就是得到一个LineReader实例
         * 通过查看API发现，想实例化一个LineReader最低要求是得到一个输入流，所以首先需要得到一个输入流
         * 输入流可以通过文件对象open（Path）方法获得，所以我们的目的变成了获取一个文件对象和一个Path
         * 文件对象可以通过get（Configuration）获取，Path可以通过FileSplit的getPath（）获得
         * Configuration可以通过context得到，FileSplit可以通过split得到---context是一直贯穿于整个过程中的，split时initialize的参数
         * 所以，将上面的思路倒序实现，就完成了初始化过程，得到一个LineReader的实例
         */
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FileSplit fileSplit = (FileSplit) split;
            Path path = fileSplit.getPath();
            fsDataInputStream = fs.open(path);
            lineReader = new LineReader(fsDataInputStream);
        }

        /**
         * nextKeyValue---写逻辑，对K和V进行赋值
         * <p>
         * 我们要实现一次读两行，而在初始化时得到的LineReader可以一次读一行，所以只需要读两次，然后赋值给Value就可以实现
         */
        public boolean nextKeyValue() throws IOException, InterruptedException {
           /* len = lineReader.readLine(text);//readLine()方法，只要把参数放进去，参数就马上有了值，值是读取的一行的内容
            text.set(new String(text.getBytes(), 0, text.getLength(), "GBK"));//转码，解决乱码问题
            sb.append(text.toString());//这里用到了StringBuffer，为了将读取两次一行的内容拼起来
            if (len == 0){
                return false;
            }
            len = lineReader.readLine(text);
            text.set(new String(text.getBytes(), 0, text.getLength(), "GBK"));
            sb.append(text.toString());
            text.set(sb.toString());
            sb.delete(0,sb.length());
            if (len == 0){
                return false;
            }*/

            /*
             * 上面的代码用到了StringBuffer，用完需要删除处理
             * 所以，可以再定义一个text2，读完两个单行之后将text和text2再set进成同一个text
             */
            len = lineReader.readLine(text);//readLine()方法，只要把参数放进去，参数就马上有了值，值是读取的一行的内容
            text.set(new String(text.getBytes(), 0, text.getLength(), "GBK"));//转码，解决乱码问题
            if (len == 0) {
                //len=0表示读完了，下面没内容了
                return false;
            }
            len = lineReader.readLine(textx);
            textx.set(new String(textx.getBytes(), 0, textx.getLength(), "GBK"));
            text.set(text.toString() + textx.toString());
            if (len == 0) {
                return false;
            }

            return true;
        }

        /**
         * 获取当前key
         */
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        /**
         * 获取当前value
         */
        public Text getCurrentValue() throws IOException, InterruptedException {
            return text;
        }

        /**
         * 获取进度
         */
        public float getProgress() throws IOException, InterruptedException {
            return true ? 0.0f : 1.0f;
        }

        /**
         * 关闭流
         */
        public void close() throws IOException {
            if (fsDataInputStream != null) {
                fsDataInputStream.close();
            }
        }
    }

    /**
     * Mapper
     */
    static class MyMapper extends Mapper<NullWritable, Text, Text, NullWritable> {

        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    /**
     * Driver
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(MyFileInputFormat.class);
        //设置Mapper类
        job.setMapperClass(MyMapper.class);
        //设置自己的输入格式类
        job.setInputFormatClass(MyInput.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.addInputPath(job, new Path("DATA/2.csv"));
        Path path = new Path("RESULT/myinput");
        path.getFileSystem(conf).delete(path, true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job, path);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
