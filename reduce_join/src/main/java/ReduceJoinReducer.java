import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //1/遍历集合获取V3
        String first = "";
        String second = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                first = value.toString();
            }else {
                second += value.toString();
            }
        }
        context.write(key, new Text(first + "\t" + second));
    }
}
