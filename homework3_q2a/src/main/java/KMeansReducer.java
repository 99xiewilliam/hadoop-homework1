import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Integer label = 1;//标记是第几个cluster
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        double[] centroid = new double[784];
        int count = 0;//计算该cluster有几个图像
        for (Text value : values) {
            count++;
            String[] split = value.toString().split(",");
            //计算平均点获取新的centroid
            for (int i = 0; i < split.length; i++) {
                centroid[i] = centroid[i] + Double.parseDouble(split[i]);
            }

        }
        //取平均值
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] = centroid[i] / count;
        }
        String str1 ="Centroid" + label + ":";
        label++;
        String str2 = Arrays.toString(centroid) + ",";
        //更新新的centroid并且记录目前cluster的数量
        context.write(new Text(str1 + str2 + count), NullWritable.get());
    }
}
