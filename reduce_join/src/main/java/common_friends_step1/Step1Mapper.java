package common_friends_step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //以冒号拆分文本数据：冒号左边是V2
        String[] split = value.toString().split(":");
        String userStr = split[0];
        //将冒号右边的字符串以逗号拆分，每个成员就是K2
        String[] split1 = split[1].split(",");
        for (String s : split1) {
            context.write(new Text(s), new Text(userStr));
        }
        //将k2和v2写入上下文中

    }
}
