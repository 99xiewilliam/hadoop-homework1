import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.base.StandardSystemProperty;
import org.apache.hadoop.fs.Path;
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

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        URI[] cacheFiles = context.getCacheFiles();
//        FileReader in = new FileReader(cacheFiles[0].getPath());
        Path[] label = context.getLocalCacheFiles();
        FileReader in = new FileReader(String.valueOf(label[0]));
        BufferedReader buffer = new BufferedReader(in);
        String str;
        String str2;
        String[] split;
        String[] split1;
        String[] split2;
        //处理随机生成的centroids数据进行分割加入到内存的list中
        while((str = buffer.readLine()) != null) {
            split = str.split("\\[");
            split1 = split[1].split("]");
            str2 = split1[0];
            split2 = str2.split(",");
            centroids.add(split2);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        Double sum = 0.0;
        Double secondNum = 0.0;
        Integer index = 0;
        Integer count = 0;
        Double minDistance = Double.POSITIVE_INFINITY;

        //遍历所有中心点
        for(String[] cent : centroids) {
            count++;
            sum = 0.0;
            //欧几里德距离，省略了开方操作，因为只是比较大小没必要开方
            for(int i = 0; i < split.length; i++) {
                //取了绝对值
                secondNum = Math.abs(Double.parseDouble(cent[i]) - Double.parseDouble(split[i]));
                secondNum = secondNum * secondNum;
                sum += secondNum;
            }
            //获取该图像到10个centroid中最小距离的centroid
            if (sum < minDistance) {
                minDistance = sum;
                index = count;
                //centroid = StringUtils.join(cent, ",");
            }
        }
        context.write(new Text(String.valueOf(index)), value);

    }
}
