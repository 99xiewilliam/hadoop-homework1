import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class HomeWork2BReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
//        for (Text value : values) {
//            context.write(value, NullWritable.get());
//            break;
//        }
        String[] split = key.toString().split(" ");
        context.write(new Text(split[0] + " " + split[1]), NullWritable.get());
    }
}
