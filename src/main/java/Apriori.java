import org.apache.hadoop.mapred.IFileOutputStream;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Apriori {
    private List<ArrayList<String>> database = new ArrayList<>();
    private Map<List<String>, Double> map = new HashMap<>();
    private Map<List<String>, Double> mapAno = new HashMap<>();
    //private Map<List<String>, Double> map_all = new HashMap<>();
    private Double min_support = 0.005;
    public static void main(String[] args) {
        Instant insOne = Instant.now();
        Apriori apriori = new Apriori();
        Map<List<String>, Double> temp_list;
        apriori.init();
        ///Users/xiexiaohao/Desktop/management/InformationEngineering/web_scale/shakespeare_basket
        ///Users/xiexiaohao/Desktop/test
        apriori.iteration(apriori.map, apriori.mapAno, "/Users/xiexiaohao/Desktop/management/InformationEngineering/web_scale/shakespeare_basket");
        int k = 40;//取前40个
        List<Map.Entry<List<String>, Double>> list = new ArrayList<>(apriori.map.entrySet());
        //根据map的value来降序排列
        Collections.sort(list, new Comparator<Map.Entry<List<String>, Double>>() {

            @Override
            public int compare(Map.Entry<List<String>, Double> o1, Map.Entry<List<String>, Double> o2) {
                int compare = o1.getValue().compareTo(o2.getValue());
                return -compare;
            }
        });
        int i = 0;
        File txt = new File("/Users/xiexiaohao/Desktop/management/InformationEngineering/web_scale/shakespeare_basket/a.txt");
        if (!txt.exists()) {
            try {
                txt.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileOutputStream fos;
        try {
            fos = new FileOutputStream(txt);
            String str;
            int b;
            byte[] bytes;
            for (Map.Entry<List<String>, Double> entry : list) {
                System.out.println(entry.getKey().toString().substring(1, entry.getKey().toString().length() - 1));
                System.out.println(entry.getValue());
                System.out.println("==================");
                str = entry.getKey().get(0) + " " + entry.getKey().get(1) + " " + entry.getValue() + "\n";
                bytes = new byte[512];
                bytes = str.getBytes();
                b = bytes.length;
                fos.write(bytes, 0, b);
                if (++i == k) {
                    break;
                }
            }
            fos.close();
            Instant insTwo = Instant.now();
            System.out.println("以毫秒计时差" + Duration.between(insOne, insTwo).toMillis());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static List<ArrayList<String>> readFiles(String filePath) {
        List<ArrayList<String>> T = new ArrayList<>();
        String str;
        File[] files = new File(filePath).listFiles();
        //切割数据集
        for (File file : files) {
            try {
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
                BufferedReader buffer = new BufferedReader(inputStreamReader);
                while ((str = buffer.readLine()) != null) {
                    T.add(new ArrayList<>(Arrays.asList(str.split(" "))));
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return T;
    }

    public void init() {
        database = readFiles("/Users/xiexiaohao/Desktop/management/InformationEngineering/web_scale/shakespeare_basket");

        //获取单个的高频集合
        for (int i = 0; i < database.size(); i++) {
            for (int j = 0; j < database.get(i).size(); j++) {
                String [] word = {database.get(i).get(j)};
                List<String> item = new ArrayList<>(Arrays.asList(word));
                if (!map.containsKey(item)) {
                    map.put(item, 1.0);
                }else {
                    map.put(item, map.get(item) + 1.0);
                }
            }
        }

        pruning(map, mapAno);
        //map_all.putAll(mapAno);

    }

    public void pruning(Map<List<String>, Double> map,
                        Map<List<String>, Double> mapAno) {
        mapAno.clear();
        mapAno.putAll(map);

        List<List<String>> delete_keys = new ArrayList<>();
        for (List<String> key : mapAno.keySet()) {
            if (mapAno.get(key) < min_support * database.size()) {
                delete_keys.add(key);
            }
        }
        for (int i = 0; i < delete_keys.size(); i++) {
            mapAno.remove(delete_keys.get(i));
        }
    }

    //把单个高频集合两两组合到一起
    public List<String> arrayUnion(List<String> list1, List<String> list2)
    {
        List<String> list = new ArrayList<>();
        list.addAll(list1);
        list.addAll(list2);
        list = new ArrayList<>(new HashSet<>(list));
        return list;
    }
    public void iteration(
            Map<List<String>, Double> map,
            Map<List<String>, Double> mapAno,
            String filePath
    ) {

        map.clear();

        List<List<String>> key_list = new ArrayList<>(mapAno.keySet());
        for (int i = 0; i < key_list.size() - 1; i++) {
            for (int j = i + 1; j < key_list.size(); j++) {
                List<String> map_item = new ArrayList<>(arrayUnion(key_list.get(i), key_list.get(j)));

                map.put(map_item, 0.0);
            }
        }

        String str;
        File[] files = new File(filePath).listFiles();
        List<String> list;
        for (File file : files) {
            try {
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
                BufferedReader buffer = new BufferedReader(inputStreamReader);
                while ((str = buffer.readLine()) != null) {
                    String[] split = str.split(" ");
                    //每行单词两两组合
                    for (int i = 0; i < split.length - 1; i++) {
                        for (int j = i + 1; j < split.length; j++) {
                            //高频的word才能两两组合
                            if (mapAno.containsKey(Arrays.asList(new String[]{split[i]})) && mapAno.containsKey(Arrays.asList(new String[]{split[j]}))) {
                                list = new ArrayList<>(Arrays.asList(new String[]{split[i], split[j]}));
                                if (map.containsKey(list) || map.containsKey(reverse(list))) {
                                    map.put(list, map.get(list) + 1.0);
                                }
                            }else {
                                continue;
                            }
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


//        for (List<String> key : map.keySet()) {
//            for (int i = 0; i < database.size(); i++) {
//                if (database.get(i).containsAll(key)) {
//                    map.put(key, map.get(key) + 1.0 / database.size());
//                }
//            }
//        }
        map.entrySet().stream().filter(col -> col.getValue() != 0.0)
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        pruning(map, mapAno);
    }

    public List<String> reverse(List<String> list) {
        //反转list以防遗漏
        Collections.reverse(list);
        return list;
    }
}