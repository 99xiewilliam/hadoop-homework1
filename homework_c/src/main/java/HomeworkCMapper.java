import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class HomeworkCMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.equals("medium_label")) {
            String[] split = value.toString().split(" ");
            if (split.length == 2) {
                context.write(new Text(split[0]), new Text(split[0] + "-" + split[1]));
            }
        }else {
            String[] split = value.toString().split(" ");
            if(split.length == 2) {
                context.write(new Text(split[0]), new Text(split[1]));
            }
        }
    }
}
