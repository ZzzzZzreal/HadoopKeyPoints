import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyComparator {

    /**
     * MyWritable
     * 根据你的数据，来写你自己的序列化类，但是这里的逻辑是有两个排序约定
     *
     * 按工作经验降序工资升序排序
     */
    static class MyWritable implements WritableComparable<MyWritable>{

        //工作经验
        String workYear;
        //工资
        int salary;

        public String getWorkYear() {
            return workYear;
        }

        public void setWorkYear(String workYear) {
            this.workYear = workYear;
        }

        public int getSalary() {
            return salary;
        }

        public void setSalary(int salary) {
            this.salary = salary;
        }

        public int compareTo(MyWritable o) {
            return 0;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(workYear);
            dataOutput.writeInt(salary);

        }

        public void readFields(DataInput dataInput) throws IOException {
            workYear = dataInput.readUTF();
            salary = dataInput.readInt();
        }

        /**
         * Comparator
         */
        static class Comparator extends WritableComparator {

            int m;

            public Comparator() {
                super(MyWritable.class, true);
            }

            @Override
            public int compare(WritableComparable a, WritableComparable b) {

                m = -((MyWritable)a).workYear.compareTo(((MyWritable)b).workYear);
                if (m==0){
                    m = (((MyWritable) a).salary-(((MyWritable) b).salary));
                }
                return m;
            }
        }

        static {
            Comparator.define(MyWritable.class, new Comparator());
        }
    }
    /**
     * Mapper
     */
    static class MyMapper extends Mapper<LongWritable,Text,MyWritable,NullWritable>{

        String[] words;
        MyWritable myWritable = new MyWritable();
        LongWritable longWritable = new LongWritable(1);
        int sal;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String arr = new String(value.getBytes(), 0, value.getLength(), "GBK");
            if (key.get() == 0) {
                return;//抛弃第一行数据
            }
            words = arr.replaceAll("\"", "").split(",");
            if (!(words[13].startsWith("1") || words[13].startsWith("2") || words[13].startsWith("3") || words[13].startsWith("4") || words[13].startsWith("5") || words[13].startsWith("6") || words[13].startsWith("7") || words[13].startsWith("8") || words[13].startsWith("9"))) {
                return;
            }
            if (words[14].contains("月")) {
                return;
            }
            myWritable.setWorkYear(words[14]);
            try {
                sal = Integer.parseInt(words[13].replaceAll("\"", "").split("-")[0].replaceAll("[a-z]", "").replaceAll("[A-Z]", ""));
                if (sal>=12)
                    myWritable.setSalary(sal);
            } catch (NumberFormatException e) {
            }
            context.write(myWritable, NullWritable.get());

        }
    }

    /**
     * Reducer
     */
    static class Myreducer extends Reducer<MyWritable,NullWritable,Text,NullWritable>{

        Text text = new Text();
        @Override
        protected void reduce(MyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
           text.set(key.getWorkYear()+"  "+key.getSalary());
            context.write(text,NullWritable.get());
        }
    }

    public static void main(String[] agrs)throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MyComparator.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(Myreducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(MyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("DATA/2.csv"));
        Path path = new Path("RESULT/comparator");
        //删除你的输出文件夹
        path.getFileSystem(conf).delete(path,true);

        FileOutputFormat.setOutputPath(job,path);

        System.exit(job.waitForCompletion(true)?0:1);
    }





}
