package mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, SortBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        SortBean sortBean = new SortBean();
        sortBean.setNum(Integer.parseInt(split[1]));
        sortBean.setWord(split[0]);
        context.write(sortBean, NullWritable.get());
    }
}
