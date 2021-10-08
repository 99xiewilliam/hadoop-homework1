import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HomeworkE1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        if (split.length == 2) {
            String k2 = split[1];
            String v2 = split[0];
            context.write(new Text(k2), new Text(v2));
        }
    }
}
