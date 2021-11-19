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
    private List<Double> labelNum = new ArrayList<>();
    //private List<String> imageLabels = new ArrayList<>();
//    private Integer countList = 0;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        System.out.println(cacheFiles[0].getPath());
        //System.out.println(cacheFiles[1].getPath());
        FileReader in = new FileReader(cacheFiles[0].getPath());
        //FileReader in1 = new FileReader(cacheFiles[1].getPath());


        BufferedReader buffer = new BufferedReader(in);
        //BufferedReader buffer1 = new BufferedReader(in1);

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
            String[] split3 = split1[1].split("@");
            //获取每个centroid对应的label
            labelNum.add(Double.parseDouble(split3[1]));
        }

//        String strImage;
//        while ((strImage = buffer1.readLine()) != null) {
//            imageLabels.add(strImage);
//        }


    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("@");
        String[] split1 = split[1].split(",");
        Double sum = 0.0;
        Double secondNum = 0.0;
        Integer index = 0;
        Integer count = 0;
        Double minDistance = Double.POSITIVE_INFINITY;

        for(String[] cent : centroids) {
            sum = 0.0;
            for(int i = 0; i < split1.length; i++) {
                secondNum = Math.abs(Double.parseDouble(cent[i]) - Double.parseDouble(split1[i]));
                secondNum = secondNum * secondNum;
                sum += secondNum;
            }
            if (sum < minDistance) {
                minDistance = sum;
                index = count;
                //centroid = StringUtils.join(cent, ",");
            }
            count++;
        }
        //System.out.println(imageLabels.get(0));
        context.write(new Text(index + "@" + labelNum.get(index)), value);
//        imageLabels.remove(0);
//        System.out.println("=========================");
//        System.out.println(imageLabels.size());
//        //countList++;

    }

}
