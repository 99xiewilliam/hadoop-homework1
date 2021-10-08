package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    //reduce将新的k2 v2专成 k3 v3，将k3 v3 写入上下文中
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //便利集合，集合中数字相加得到v3
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        ///将k3 v3写入上下文中
        context.write(key, new LongWritable(count));

    }
}
