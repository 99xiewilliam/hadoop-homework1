import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;

public class HomeworkB1Reducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, List<String>> map = new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<>();

        for (Text value : values) {
            list.add(value.toString());
        }
        map.put(key.toString(), list);
        //context.write(key, new Text(buffer.toString().substring(0, buffer.toString().length() - 1)));
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> keySet = map.keySet();
        for (String k : keySet) {
            for (String k1 : keySet) {
                if (!k.equals(k1)) {
                    Double similarity = null;
                    List<String> list = new ArrayList<>(map.get(k));
                    List<String> list1 = new ArrayList<>(map.get(k1));
                    list.retainAll(list1);
                    int size = map.get(k).size();
                    int size1 = map.get(k1).size();
                    int collect = size + size1 - list.size();
                    if (collect == 0) {
                        similarity = 0.0;
                    }else {
                        similarity = Double.valueOf(list.size()) / Double.valueOf(collect);
//                        if (similarity > 0) {
//                            context.write(new Text(k + "-" + k1), new Text(similarity.toString() + "-" + String.join(",", list)));
//                        }
                        context.write(new Text(k + "-" + k1), new Text(similarity.toString() + "-" + String.join(",", list)));
                    }
                }
            }
        }
    }

//    public static <T> List<T> deepCopy(List<T> src) throws IOException, ClassNotFoundException {
//        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//        ObjectOutputStream out = new ObjectOutputStream(byteOut);
//        out.writeObject(src);
//
//        ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
//        ObjectInputStream in = new ObjectInputStream(byteIn);
//
//        @SuppressWarnings("unchecked")
//        List<T> dest = (List<T>) in.readObject();
//        return dest;
//    }
}
