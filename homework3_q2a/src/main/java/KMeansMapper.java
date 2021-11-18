import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.base.StandardSystemProperty;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    private List<String[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileReader in = new FileReader(cacheFiles[0].getPath());
        BufferedReader buffer = new BufferedReader(in);
        String str;
        String str2;
        String[] split;
        String[] split1;
        String[] split2;
        while((str = buffer.readLine()) != null) {
            split = str.split("\\[");
            split1 = split[1].split("]");
            str2 = split1[0];
            split2 = str2.split(",");
            centroids.add(split2);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String centroid = "";
        Double sum = 0.0;
        Double secondNum = 0.0;
        Integer index = 0;
        Integer count = 0;
        Double minDistance = Double.POSITIVE_INFINITY;

        for(String[] cent : centroids) {
            count++;
            sum = 0.0;
            for(int i = 0; i < split.length; i++) {
                secondNum = Math.abs(Double.parseDouble(cent[i]) - Double.parseDouble(split[i]));
                secondNum = secondNum * secondNum;
                sum += secondNum;
            }
            if (sum < minDistance) {
                minDistance = sum;
                index = count;
                //centroid = StringUtils.join(cent, ",");
            }
        }
        context.write(new Text(String.valueOf(index)), value);

    }
}
