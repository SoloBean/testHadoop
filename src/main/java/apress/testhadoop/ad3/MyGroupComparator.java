package apress.testhadoop.ad3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    public MyGroupComparator(){
        super(UserAndIdWritable.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b){
        UserAndIdWritable first = (UserAndIdWritable) a;
        UserAndIdWritable second = (UserAndIdWritable) b;

        return first.User.compareTo(second.User);
    }
}
