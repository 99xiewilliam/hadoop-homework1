import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration = null;
    private FileSplit fileSplit = null;
    private boolean processed = false;
    private BytesWritable bytesWritable = new BytesWritable();
    private FSDataInputStream inputStream = null;
    private FileSystem fileSystem = null;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取文件的切片
        fileSplit = (FileSplit) inputSplit;

        //获取Configuration
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed){
            //获取源文件字节输入流
            //1。1获取源文件文件系统
            fileSystem = FileSystem.get(configuration);
            inputStream = fileSystem.open(fileSplit.getPath());
            //1。2通过filesystem获取文件字节输入流
            //读取源文件数据到普通字节数组
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(inputStream, bytes, 0, (int)fileSplit.getLength());
            //将字节数组中数据封装到BytesWritable
            bytesWritable.set(bytes, 0, (int)fileSplit.getLength());
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }


}
