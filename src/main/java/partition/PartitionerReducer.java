package partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    K2: Text
    V2: NullWritable

    K3: Text
    V3: NullWritable
 */
public class PartitionerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    public static enum Counter {
        MY_INPUT_RECORDERS, MY_INPUT_BYTES
    }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.MY_INPUT_RECORDERS).increment(1L);
        context.write(key, NullWritable.get());
    }
}
