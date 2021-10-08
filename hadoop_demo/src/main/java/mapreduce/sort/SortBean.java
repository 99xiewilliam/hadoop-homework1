package mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private int num;

    //先排word再排数字升序排列
    @Override
    public int compareTo(SortBean o) {
        int i = this.word.compareTo(o.word);
        if (i == 0) {
            return this.num - o.num;
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num = dataInput.readInt();
    }

    public void setWord(String word) {
        this.word = word;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getWord() {
        return word;
    }

    public int getNum() {
        return num;
    }

    @Override
    public String toString() {
        return word + "\t" + num;
    }
}
