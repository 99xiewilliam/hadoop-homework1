import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Integer label = 1;
    private Integer realLabelNum = 0;

    private Map<String, Integer> map = new HashMap<>();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Integer max = 0;
        double[] centroid = new double[784];
        int count = 0;
        String imageLabel;
        map.put("0.0", 0);
        map.put("1.0", 0);
        map.put("2.0", 0);
        map.put("3.0", 0);
        map.put("4.0", 0);
        map.put("5.0", 0);
        map.put("6.0", 0);
        map.put("7.0", 0);
        map.put("8.0", 0);
        map.put("9.0", 0);


        for (Text value : values) {
            count++;
            String[] split = value.toString().split("@");
            imageLabel = split[0];
            if (map.containsKey(imageLabel)) {
                map.put(imageLabel, map.get(imageLabel) + 1);
            }
            String[] split1 = split[1].split(",");
            for (int i = 0; i < split1.length; i++) {
                centroid[i] = centroid[i] + Double.parseDouble(split1[i]);
            }

        }
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] = centroid[i] / count;
        }

        Set<String> set = map.keySet();
        String maxLabel = null;
        String[] split = key.toString().split("@");

        for (String s : set) {
            System.out.println("****************");
            System.out.println((double)map.get(s) / count);
            System.out.println(s);
            if (s.equals(split[1])) {
                realLabelNum = map.get(s);
            }
            if (map.get(s) > max) {
                max = map.get(s);
                maxLabel = s;
            }
        }
        String str1 ="Centroid" + label + ":";
        label++;
        String str2 = Arrays.toString(centroid) + ",";
        Double percent = (double)max / count;
        //String[] split = key.toString().split("@");

        context.write(new Text(str1 + str2 + count + "," + maxLabel + "," + percent + "," + realLabelNum + "@" + split[1]), NullWritable.get());
    }
}
