import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MyOutputFormat {
    /**
     * MyWritable
     */
    static class MyWritable implements WritableComparable<MyWritable> {

        //使用时长
        static long usedDays;
        //品牌
        String brand;
        //每个品牌的数量
        static long count;
        //平均使用时长
        int avgDays;

        public long getUsedDays() {
            return usedDays;
        }

        public void setUsedDays(long usedDays) {
            MyWritable.usedDays = usedDays;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            MyWritable.count = count;
        }

        public int getAvgDays() {
            return avgDays;
        }

        public void setAvgDays(double avgDays) {
            this.avgDays = (int) avgDays;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public int compareTo(MyWritable o) {
            return this.brand.compareTo(o.brand);
        }

        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeLong(usedDays);
            dataOutput.writeUTF(brand);
//            dataOutput.writeLong(avgDays);
        }

        public void readFields(DataInput dataInput) throws IOException {

            usedDays = dataInput.readLong();
            brand = dataInput.readUTF();
//            dataInput.readLong();
        }

        @Override
        public String toString() {
            return this.getBrand()+" "+this.getAvgDays();
        }
    }


    /**
     * Mapper
     */
    static class MyMapper extends Mapper<LongWritable,Text,MyWritable,MyWritable> {

        String[] words;
        MyWritable myWritable = new MyWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//            System.out.println(value);
//            String arr = new String(value.getBytes(), 0, value.getLength(), "GBK");
//            System.out.println(arr);
            if (key.get() == 0) {
                return;//抛弃第一行数据
            }
            words = value.toString().split(",");
            //words[5]是安装日期，words[6]是预约日期，words[13]是品牌，words[26]是实际服务类型
            if (words.length<27 || words[13] == null||words[5].length()==0||words[5].length()==1||words[6].length()==0||words[6].length()==1) {
                return;//抛弃掉不知道存不存在的空指
            }
            if (words[26].contains("维修")) {
                //计算使用时长
                try {
                    myWritable.usedDays = (new SimpleDateFormat("yy/MM/dd").parse(words[6]).getTime()-new SimpleDateFormat("yy/MM/dd").parse(words[5]).getTime())/86400000l;
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                myWritable.setBrand(words[13]);
                context.write(myWritable,myWritable);
            }

        }

    }

    /**
     * Reducer
     */
    static class MyReducer extends Reducer<MyWritable,MyWritable,Text,NullWritable> {

        Text text = new Text();
        @Override
        protected void reduce(MyWritable key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {
            //统计每个品牌的数量
            key.count = 0;
            long sum = 0;
            for (MyWritable myWritable: values) {
                sum+=key.getUsedDays();
                key.count++;
            }
            key.setAvgDays((double) sum/key.count);
            text.set(key.toString());
            context.write(text,NullWritable.get());
        }
    }

    /**
     * 自定义输出格式
     */
    static class MyOutput extends FileOutputFormat<Text,NullWritable> {

        private String prefix = "myout_";
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            // 新建一个可写入的文件
            Path outputDir = FileOutputFormat.getOutputPath(taskAttemptContext);
            String subfix = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
            System.out.println();
            Path path = new Path(outputDir.toString()+"/"+prefix+subfix.substring(subfix.length()-5, subfix.length()));
            FSDataOutputStream fileOut = path.getFileSystem(taskAttemptContext.getConfiguration()).create(path);

            return new MyRWer(fileOut);
        }
    }
    static class MyRWer extends RecordWriter<Text,NullWritable>{

        String[] words;
        Map map = new TreeMap<Integer,String>();
        StringBuffer sb = new StringBuffer();
        private PrintWriter out;

        public MyRWer(FSDataOutputStream fileOut) {
            out = new PrintWriter(fileOut);
        }

        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

            sb.delete(0,sb.length());
            words = text.toString().replaceAll("/n"," ").split(" ");
            for(int i=0;i<words.length;){
                map.put(Integer.parseInt(words[i+1]),words[i]);
                i+=2;
            }
            Iterator<Map.Entry> iterator = map.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry mm=iterator.next();
                sb.append(mm.getValue()+" "+mm.getKey()+"\n");
            }
            if (sb.toString().contains("康拜恩")) {
                out.println(sb);
            }
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            out.close();
        }
    }

    /**
     * Driver
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建一个Configuration实例
        Configuration conf = new Configuration();
        //根据conf来获得一个Job的实例，通过这个Job来实现任务的设置
        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(MyOutputFormat.class);
        //设置Mapper类
        job.setMapperClass(MyMapper.class);
        //设置Reducer类
        job.setReducerClass(MyReducer.class);

        job.setOutputFormatClass(MyOutput.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(MyWritable.class);
        job.setMapOutputValueClass(MyWritable.class);

        job.setNumReduceTasks(1);//设置你的reduceTask数量，有几个就会生出几个文件

        FileInputFormat.addInputPath(job, new Path("DATA/2.csv"));
        Path path = new Path("RESULT/myoutput");
        path.getFileSystem(conf).delete(path, true);//删除你的输出文件夹

        FileOutputFormat.setOutputPath(job, path);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}

