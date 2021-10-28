import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HomeWork2DMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<List<String>, Double> map = new HashMap<>();
    private Map<List<String>, Double> mapAno = new HashMap<>();
    private Integer baskets = 0;
    private Double min_support = 0.005;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileReader in = new FileReader(cacheFiles[0].getPath());
        BufferedReader buffer = new BufferedReader(in);
        String str;
        List<String> set;
        while((str = buffer.readLine()) != null) {
            String[] split = str.split(" ");
            set = new ArrayList<>(Arrays.asList(split));
            map.put(set, 0.0);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        List<String> set;
        for (int i = 0; i < split.length - 1; i++) {
            for (int j = i + 1; j < split.length; j++) {
                set = new ArrayList<>(Arrays.asList(new String[]{split[i], split[j]}));
                if (map.containsKey(set)|| map.containsKey(reverse(set))) {
                    map.put(set, map.get(set) + 1.0);
                }
            }
        }
        baskets++;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        Set<List<String>> sets = map.keySet();
        List<String> list;
        List<String> list1 = new ArrayList<>();
        list1.add("thy");
        list1.add("thee");
        for (List<String> set : sets) {
            if (set.containsAll(list1) || set.containsAll(reverse(list1))) {
                System.out.println("=-=-====-=-=--==-=-=-=-==-=-=--=-=-=-=-");
                System.out.println(map.get(set));
                System.out.println(baskets*min_support);
            }
//            if (map.get(set) >= min_support * baskets) {
//                context.write(new Text(set.get(0) + " " + set.get(1)), new Text(map.get(set).toString()));
//            }
            context.write(new Text(set.get(0) + " " + set.get(1)), new Text(map.get(set).toString()));
        }
    }
    public List<String> reverse(List<String> list) {
        Collections.reverse(list);
        return list;
    }

//    public void getTable(String[] split) {
//        database.add(Arrays.asList(split));
//    }
//
//    public void pruning() {
//        mapAno.clear();
//
//        for (Set<String> set : map.keySet()) {
//            if (map.get(set) >= min_support * baskets) {
//                mapAno.put(set, map.get(set));
//            }
//        }
//    }
//
//    public void init() {
//        baskets = database.size();
//
//        for (int i = 0; i < baskets; i++) {
//            for (int j = 0; j < database.get(i).size(); j++) {
//                Set<String> set = new HashSet<>(Arrays.asList(new String[]{database.get(i).get(j)}));
//                if (!map.containsKey(set)) {
//                    map.put(set, 1.0);
//                }else {
//                    map.put(set, map.get(set) + 1.0);
//                }
//            }
//        }
//
//        pruning();
//    }
//
//    public void iteration() {
//        System.out.println("baskets size===============================");
//        System.out.println(baskets);
//
//        map.clear();
//        Integer hashValue;
//        for (List<String> row : database) {
//            for (int i = 0; i < row.size() - 1; i++) {
//                for (int j = i + 1; j < row.size(); j++) {
//                    hashValue = (row.get(i) + row.get(j)).hashCode();
//                    if (!hashTable.containsKey(hashValue)) {
//                        hashTable.put(hashValue, 1.0);
//                    }else {
//                        hashTable.put(hashValue, hashTable.get(hashValue) + 1.0);
//                    }
//                }
//            }
//        }
//
//        Set<Set<String>> sets = mapAno.keySet();
//
//        List<String> list;
//        List<String> list1;
//        List<String> listAll;
//        Set<String> setAll;
//        Integer hashValueAno;
//        for (Set<String> set : sets) {
//            for (Set<String> set1 : sets) {
//                if (!set.containsAll(set1)) {
//                    list = new ArrayList<>(set);
//                    list1 = new ArrayList<>(set1);
//                    hashValueAno = (list.get(0) + list1.get(0)).hashCode();
//                    if (hashTable.containsKey(hashValueAno) && hashTable.get(hashValueAno) >= min_support * baskets) {
//                        listAll = new ArrayList<>(Arrays.asList(new String[]{list.get(0), list1.get(0)}));
//                        setAll = new HashSet<>(listAll);
//                        map.put(setAll, 1.0);
//                    }
//                }
//            }
//        }
//
//
//    }
}
