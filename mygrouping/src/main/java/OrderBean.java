import com.sun.tools.corba.se.idl.constExpr.Or;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public Double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public int compareTo(OrderBean o) {
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0) {
            i = this.price.compareTo(o.price) * -1;
        }
        return i;
    }
}
