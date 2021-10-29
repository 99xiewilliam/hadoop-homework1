import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class HomeWork2BMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<Set<String>, String> map = new HashMap<>();
    private Integer baskets = 0;
    private List<List<String>> database = new ArrayList<>();
    private Map<Set<String>, Double> mapAno = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileReader in = new FileReader(cacheFiles[0].getPath());
        BufferedReader bufferedReader = new BufferedReader(in);
        String str;

        while((str = bufferedReader.readLine()) != null) {
            String[] split = str.split(" ");
            Set<String> set = new HashSet<>(Arrays.asList(new String[]{split[0], split[1]}));
            map.put(set, "1");
        }
        bufferedReader.close();
        in.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
//        getTable(split);
        Set<String> set1;
        for (int i = 0; i < split.length - 1; i++) {
            for (int j = i + 1; j < split.length; j++) {
                context.write(new Text(split[i] + " " + split[j]), new Text("1"));
            }
        }
        baskets++;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("baskets"), new Text(baskets.toString()));
        System.out.println("test====================================");
        System.out.println(baskets);
//        init();
//        iteration();
//        Set<Set<String>> sets = mapAno.keySet();
//        List<String> list;
//        for (Set<String> set : sets) {
//            list = new ArrayList<>(set);
//            context.write(new Text(list.get(0) + " " + list.get(1)), new Text(mapAno.get(set).toString()));
//        }
    }
//    private Map<String, String> map = new HashMap<>();
//    private Integer baskets = 0;
//    @Override
//    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        //获取缓存中的uri
//        URI[] uris = context.getCacheFiles();
//        //Path cacheFile = new Path(uris[0]);
//        System.out.println(uris[0]);
//        FileReader in = new FileReader(uris[0].getPath());
//        BufferedReader reader = new BufferedReader(in);
//
//        String str;
//        while ((str = reader.readLine()) != null) {
//            String[] split = str.split(" ");
//            map.put(split[0], split[1]);
//        }
//        reader.close();
//        in.close();
//    }
//
//    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        String[] split = value.toString().split(" ");
//        for (int i = 0; i < split.length - 1; i++) {
//            for (int j = i + 1; j < split.length; j++) {
//                if (map.containsKey(split[i]) && map.containsKey(split[j])) {
//                    context.write(new Text(split[i] + " " + split[j]), new Text("1"));
//                }
//            }
//        }
//        baskets++;
//
//    }
//
//    //计算总共多少行
//    @Override
//    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        context.write(new Text("baskets"), new Text(baskets.toString()));
//    }
}
