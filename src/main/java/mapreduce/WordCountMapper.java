package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //map方法就是把k1 v1转为k2 v2
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        //将一行的文本数据进行拆分
        String[] split = value.toString().split(".");

        //便利数组，组装k2和v2
        for (String s : split) {
            //将k2 v2写入上下文
            text.set(s);
            longWritable.set(1);
            context.write(text, longWritable);
        }

    }
}
