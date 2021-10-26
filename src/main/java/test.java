import java.util.*;

public class test {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        List<String> list1 = new ArrayList<>();
        Map<List<String>, Double> map = new HashMap<>();
        list.add("1");
        list.add("2");
        list1.add("1");
        list1.add("2");
        map.put(list, 0.1);
        if (map.containsKey(list1) || map.containsKey(reverse(list1))) {
            System.out.println("1");
        }else {
            System.out.println("0");
        }

    }

    public static List<String> reverse(List<String> list) {
        Collections.reverse(list);
        return list;
    }
}
