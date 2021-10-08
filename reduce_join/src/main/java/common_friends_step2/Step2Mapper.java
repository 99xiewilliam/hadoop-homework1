package common_friends_step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //1.拆分行文本数据，得到v2
        String[] split = value.toString().split("\t");
        String friendStr = split[1];
        //继续以"-"为分隔符拆分行文本数据第一部分，得到数组
        String[] userArray = split[0].split("-");
        //对数组做一个排序
        Arrays.sort(userArray);
        //对数组中的元素进行两两组合，得到k2
        for (int i = 0; i < userArray.length - 1; i++) {
            for (int j = i + 1; j < userArray.length; j++) {
                //将k2和v2写入上下文
                context.write(new Text(userArray[i] + "-" + userArray[j]), new Text(friendStr));
            }
        }
    }
}
