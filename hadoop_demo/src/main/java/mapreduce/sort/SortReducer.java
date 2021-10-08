package mapreduce.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {

    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Reducer<SortBean, NullWritable, SortBean, NullWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("");
        context.write(key, NullWritable.get());
    }
}
