import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * 练习HDFS的shell命令
 * 这些代码没法实时演示出结果，自己做吧
 */
public class HdfsTest {

    static FileSystem fileSystem = null;
    /**
     * 初始化fileSystem，得到一个HDFS的文件系统实例
     */
    @Before
    public void init() {

        try {
            Configuration configuration = new Configuration();
            URI uri = new URI("hdfs://127.0.0.1:9000");
            fileSystem = FileSystem.get(uri, configuration, "root");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void hdfsMkdir() {

        try {
            fileSystem.mkdirs(new Path("/demo/HdfsTest"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件
     */
    @Test
    public void hdfsCreate() {

        try {
            fileSystem.create(new Path("/demo/HdfsTest/a.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件
     */
    @Test
    public void hdfsUpload() {

        try {
            fileSystem.copyFromLocalFile(new Path("/home/sker/aabbbc/hadoop/aaa.png"),new Path("/demo/HdfsTest"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载文件
     */
    @Test
    public void hdfsDownload() {

        try {
            fileSystem.copyToLocalFile(new Path("/demo/HdfsTest/aaa.png"),new Path("/tmp"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void hdfsDelete() {

        try {
            fileSystem.delete(new Path("/test/aa.txt"),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取文件---cat
     */
    @Test
    public void hdfsCat(){

        try {
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/demo/a.txt"));
            byte[] b = new byte[1024];
            int len = 0;
            while ((len = fsDataInputStream.read(b))!=-1){
                fsDataInputStream.read(b);
                String arr = new String(b,0,len);
                System.out.println(arr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * list---获取文件列表
     */
    @Test
    public void hdfsList() throws Exception {

        FileStatus[] fs = fileSystem.listStatus(new Path("/demo"));
        for (FileStatus fileStatus:fs) {
            System.out.println(fileStatus.getPath().getName());
        }
    }

    /**
     * 获取文件列表
     * @throws Exception
     */
    @Test
    public void demo() throws Exception {
        HdfsTest.listFiles(new Path("/"));
    }
    public static void listFiles(Path path)throws Exception {
        FileStatus[] fslist = fileSystem.listStatus(path);

        for (FileStatus fs:fslist ) {
            if(fs.isDirectory()){

                listFiles(fs.getPath());
            }
            System.out.println(fs.getPath().getName());
        }
    }

    /**
     * cat----取巧的办法，不建议使用。
     * 正常的使用方法是上面cat的那种方法，定义数组长度为1024的倍数，然后进循环，这样才是标准的。
     */
    @Test
    public void  hdfsCatDemo() throws Exception {

        Path path = new Path("/demo/a.txt");
        byte[] b = new byte[(int)fileSystem.getFileLinkStatus(path ).getLen()];
       FSDataInputStream fsDataInputStream = fileSystem.open(path);
       fsDataInputStream.read(b);
        System.out.println(new String(b));
    }


}
