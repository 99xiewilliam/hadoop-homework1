import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Integer label = 1;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        double[] centroid = new double[784];
        int count = 0;
        for (Text value : values) {
            count++;
            String[] split = value.toString().split(",");
            for (int i = 0; i < split.length; i++) {
                centroid[i] = centroid[i] + Double.parseDouble(split[i]);
            }

        }
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] = centroid[i] / count;
        }
        String str1 ="Centroid" + label + ":";
        label++;
        String str2 = Arrays.toString(centroid) + ",";
        context.write(new Text(str1 + str2 + count), NullWritable.get());
    }
}
