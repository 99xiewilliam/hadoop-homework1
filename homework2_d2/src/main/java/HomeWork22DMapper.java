import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HomeWork22DMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<Set<String>, Double> map = new ConcurrentHashMap<>();
    private Map<Set<String>, Double> mapAno = new ConcurrentHashMap<>();
    private Map<Integer, Double> hashTable = new ConcurrentHashMap<>();
    private Integer baskets = 0;
    private List<List<String>> database = new ArrayList<>();
    private Double min_support = 0.005;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        getTable(split);
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        init();
        iteration();
        List<String> list;
        for (Map.Entry<Set<String>, Double> entry : map.entrySet()) {
            list = new ArrayList<>(entry.getKey());
            context.write(new Text(list.get(0) + " " + list.get(1)), new Text("1"));
        }
    }

    public void getTable(String[] split) {
        database.add(Arrays.asList(split));
    }

    public void pruning() {
        mapAno.clear();

        for (Set<String> set : map.keySet()) {
            if (map.get(set) >= min_support * baskets) {
                mapAno.put(set, map.get(set));
            }
        }
    }

    public void init() {
        baskets = database.size();
        for (int i = 0; i < baskets; i++) {
            for (int j = 0; j < database.get(i).size(); j++) {
                Set<String> set = new HashSet<>(Arrays.asList(new String[]{database.get(i).get(j)}));
                if (!map.containsKey(set)) {
                    map.put(set, 1.0);
                }else {
                    map.put(set, map.get(set) + 1.0);
                }
            }
        }

        pruning();
    }

    public void iteration() {
        System.out.println("baskets size===============================");
        System.out.println(baskets);

        map.clear();
        Integer hashValue;
        for (List<String> row : database) {
            for (int i = 0; i < row.size() - 1; i++) {
                for (int j = i + 1; j < row.size(); j++) {
                    hashValue = (row.get(i) + row.get(j)).hashCode();
                    if (!hashTable.containsKey(hashValue)) {
                        hashTable.put(hashValue, 1.0);
                    }else {
                        hashTable.put(hashValue, hashTable.get(hashValue) + 1.0);
                    }
                }
            }
        }

        Set<Set<String>> sets = mapAno.keySet();

        List<String> list;
        List<String> list1;
        List<String> listAll;
        Set<String> setAll;
        Integer hashValueAno;
        for (Set<String> set : sets) {
            for (Set<String> set1 : sets) {
                if (!set.containsAll(set1)) {
                    list = new ArrayList<>(set);
                    list1 = new ArrayList<>(set1);
                    hashValueAno = (list.get(0) + list1.get(0)).hashCode();
                    if (hashTable.containsKey(hashValueAno) && hashTable.get(hashValueAno) >= min_support * baskets) {
                        listAll = new ArrayList<>(Arrays.asList(new String[]{list.get(0), list1.get(0)}));
                        setAll = new HashSet<>(listAll);
                        map.put(setAll, 1.0);
                    }
                }
            }
        }


    }
}
