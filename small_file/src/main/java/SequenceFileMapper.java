import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        //获取文件的名字,k2
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        //将k2 v2写入上下文中
        context.write(new Text(fileName), value);
    }
}
