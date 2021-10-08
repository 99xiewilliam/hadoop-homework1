package partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {
    /*
        1。定义分区规则
        2。返回对应分区编号
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] split = text.toString().split("\t");
        String numStr = split[5];

        if (Integer.parseInt(numStr) > 15) {
            return 1;
        }else {
            return 0;
        }
    }
}
