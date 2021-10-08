package map_join;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> map = new HashMap<>();
    //将分布式缓存的小表数据读取到本地map集合中
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //获取分布式缓存文件列表
        URI[] cacheFiles = context.getCacheFiles();

        //获取指定分布式缓存文件的文件系统
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());

        //获取文件的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(""));
        //读取文件内容，并将数据存入Map集合 4。1将字节输入流转为字符缓冲流 FSDataInputStream --》BufferedReader
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        //4。2读取小表文件内容，以行为单位，并将读取的数据存入map集合


        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(",");
            map.put(split[0], line);
        }
        //关闭流
        bufferedReader.close();
        fileSystem.close();
    }

    //对大表的处理业务逻辑，而且要实现大表和小表的join操作
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //行文本数据中获取商品id：p0001得到K2
        String[] split = value.toString().split(",");
        String productId = split[2];
        //在map集合中，将商品的id作为键，获取值（商品的行文本数据），将value和值拼接得到V2
        String productLine = map.get(productId);
        String valueLine = productLine + "\t" + value.toString();
        //将K2 V2写入上下文
        context.write(new Text(productId), new Text(valueLine));

    }
}
