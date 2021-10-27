import java.util.*;

public class test {
    public static void main(String[] args) {
        Map<Set<String>, Double> map = new HashMap<>();
//        List<String> list = new ArrayList<>();
//        List<String> list1 = new ArrayList<>();
//        Map<List<String>, Double> map = new HashMap<>();
//        list.add("1");
//        list.add("3");
//        list.add("2");
//        list1.add("2");
//        list1.add("1");
//        boolean b = list.containsAll(list1);
//        System.out.println(b);
        Set<String> set = new HashSet<>();
        set.add("1");
        set.add("3");
        map.put(set, 1.0);
        Set<String> set1 = new HashSet<>();
        set1.add("3");
        set1.add("1");
        if (map.containsKey(set1)) {
            System.out.println("yes");
        }

    }

    public static List<String> reverse(List<String> list) {
        Collections.reverse(list);
        return list;
    }
}
