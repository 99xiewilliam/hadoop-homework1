import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class HomeWork2BMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> map = new HashMap<>();
    private Integer baskets = 0;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //获取缓存中的uri
        URI[] uris = context.getCacheFiles();
        //Path cacheFile = new Path(uris[0]);
        System.out.println(uris[0]);
        FileReader in = new FileReader(uris[0].getPath());
        BufferedReader reader = new BufferedReader(in);

        String str;
        while ((str = reader.readLine()) != null) {
            String[] split = str.split(" ");
            map.put(split[0], split[1]);
        }
        reader.close();
        in.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (int i = 0; i < split.length - 1; i++) {
            for (int j = i + 1; j < split.length; j++) {
                if (map.containsKey(split[i]) && map.containsKey(split[j])) {
                    context.write(new Text(split[i] + " " + split[j]), new Text("1"));
                }
            }
        }
        baskets++;

    }

    //计算总共多少行
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("baskets"), new Text(baskets.toString()));
    }
}
