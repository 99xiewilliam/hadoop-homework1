import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class HomeWork21BMapper extends Mapper<LongWritable, Text, Text, Text> {
    //    private Double min_support = 0.005;
//    private Integer baskets = 0;
//    private Map<String, Double> map = new HashMap<>();
    private Map<Set<String>, Double> map = new HashMap<>();
    private Integer baskets = 0;
    private List<List<String>> database = new ArrayList<>();
    private Map<Set<String>, Double> mapAno = new HashMap<>();
    private Double min_support = 0.005;
    private Integer basketsNum = 0;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (int i = 0; i < split.length; i++) {
            String[] word = { split[i] };
            Set<String> item = new HashSet<>(Arrays.asList(word));
            if (!map.containsKey(item)) {
                map.put(item, 1.0);
            }else {
                map.put(item, map.get(item) + 1.0);
            }
        }
        basketsNum++;
        getTable(split);
//        for (int i = 0; i < split.length - 2; i++) {
//            for (int j = i + 1; j < split.length - 1; j++) {
//                for (int k = j + 1; k < split.length; k++) {
//                    Set<String> set1 = new HashSet<>(Arrays.asList(new String[]{split[i], split[j]}));
//                    Set<String> set2 = new HashSet<>(Arrays.asList(new String[]{split[i], split[k]}));
//                    Set<String> set3 = new HashSet<>(Arrays.asList(new String[]{split[j], split[k]}));
//                    if (map.containsKey(set1) && map.containsKey(set2) && map.containsKey(set3)) {
//                        context.write(new Text(split[i] + " " + split[j] + " " + split[k]), new Text("1"));
//                    }
//                }
//            }
//        }
//        baskets++;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        context.write(new Text("baskets"), new Text(baskets.toString()));
//        System.out.println("test====================================");
//        System.out.println(baskets);
        init();
        iteration();
        Set<Set<String>> sets = map.keySet();
        List<String> list;
        for (Set<String> set : sets) {
            list = new ArrayList<>(set);
            context.write(new Text(list.get(0) + " " + list.get(1)), new Text(map.get(set).toString()));
        }
    }
    public void getTable(String[] split) {
        List<String> list = Arrays.asList(split);
        database.add(list);
    }
    public void pruning() {
        mapAno.clear();

        for (Set<String> string: map.keySet()) {
            if (map.get(string) >= min_support * basketsNum) {
                mapAno.put(string, map.get(string));
            }
        }

    }
    public void init() {
        //basketsNum = database.size();
//        for (int i = 0; i < database.size(); i++) {
//            for (int j = 0; j < database.get(i).size(); j++) {
//                String[] word = { database.get(i).get(j) };
//                Set<String> item = new HashSet<>(Arrays.asList(word));
//                if (!map.containsKey(item)) {
//                    map.put(item, 1.0);
//                }else {
//                    map.put(item, map.get(item) + 1.0);
//                }
//            }
//        }
        pruning();
    }

    public void iteration() {
        map.clear();

        List<Set<String>> key_list = new ArrayList<>(mapAno.keySet());
        for (int i = 0; i < key_list.size() - 1; i++) {
            for (int j = i + 1; j < key_list.size(); j++) {
                Set<String> set_item = new HashSet<>(arrayUnion(key_list.get(i), key_list.get(j)));
                map.put(set_item, 0.0);
            }
        }
        Set<String> set1;
        Set<String> set2;
        Set<String> set3;
        for (List<String> row : database) {
            for (int i = 0; i < row.size() - 1; i++) {
                for (int j = i + 1; j < row.size(); j++) {
                    set1 = new HashSet<>(Arrays.asList(new String[]{row.get(i)}));
                    set2 = new HashSet<>(Arrays.asList(new String[]{row.get(j)}));
                    if (mapAno.containsKey(set1) && mapAno.containsKey(set2)) {
                        set3 = new HashSet<>(Arrays.asList(new String[]{row.get(i), row.get(j)}));
                        if (map.containsKey(set3)) {
                            map.put(set3, map.get(set3) + 1.0);
                        }
                    }else {
                        continue;
                    }
                }
            }
        }

        pruning();



    }
    public Set<String> arrayUnion(Set<String> set1, Set<String> set2) {
        Set<String> set = new HashSet<>();
        set.addAll(set1);
        set.addAll(set2);
        return set;
    }
//    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        String[] split = value.toString().split(" ");
//        for (String s : split) {
//            if (!map.containsKey(s)) {
//                map.put(s, 0.0);
//            }else {
//                map.put(s, map.get(s) + 1.0);
//            }
//        }
//        baskets++;
//    }
//
//    @Override
//    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        Set<String> strings = map.keySet();
//        for (String string : strings) {
//            if (map.get(string) >= min_support * baskets) {
//                context.write(new Text(string), new Text(string + " " + map.get(string).toString()));
//            }
//        }
//        System.out.println("test!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//        System.out.println(baskets);
//    }
}
