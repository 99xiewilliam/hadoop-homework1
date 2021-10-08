import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class HomeworkD2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String v2 = null;
        if (split.length == 2) {
            v2 = split[1];
            String[] split1 = split[0].split("-");
            int[] array = Arrays.asList(split1).stream().mapToInt(Integer::parseInt).toArray();
            Arrays.sort(array);
            for (int i = 0; i < array.length - 1; i++) {
                for (int j = i + 1; j < array.length; j++) {
                    context.write(new Text(array[i] + "-" + array[j]), new Text(v2));
                }
            }
        }
    }
}
