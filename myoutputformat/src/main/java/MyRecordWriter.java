import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream fsDataOutputStream = null;
    private FSDataOutputStream fsDataOutputStream_bad = null;

    public MyRecordWriter() {

    }

    public MyRecordWriter(FSDataOutputStream fsDataOutputStream, FSDataOutputStream fsDataOutputStream_bad) {
        this.fsDataOutputStream = fsDataOutputStream;
        this.fsDataOutputStream_bad = fsDataOutputStream_bad;
    }
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //从文本数据中获取第9个字段
        String[] split = text.toString().split("\t");
        String number = split[9];
        //根据字段的值，判断评论的类型，然后将乙瑛的数据写入不同的文件夹文件中
        if (Integer.parseInt(number) <= 1) {
            fsDataOutputStream.write(text.toString().getBytes());
            fsDataOutputStream.write("\r\n".getBytes());
        }else {
            fsDataOutputStream_bad.write(text.toString().getBytes());
            fsDataOutputStream_bad.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }
}
