package common_friends_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step1Reducer extends Reducer <Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //遍历集合，并将每个元素拼接，得到k3
        StringBuffer buffer = new StringBuffer();
        //k2就是v3
        for (Text value : values) {
            buffer.append(value.toString()).append("-");
        }
        //将k3和v3写入上下文
        context.write(new Text(buffer.toString()), key);
    }
}
