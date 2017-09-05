package apress.testhadoop.ad3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserAndIdWritable implements WritableComparable<UserAndIdWritable> {
    public Text User = new Text();
    public Text ID = new Text();

    public UserAndIdWritable(){

    }

    public UserAndIdWritable(Text User, Text ID){
        this.User = User;
        this.ID = ID;
    }

    public void write(DataOutput out) throws IOException {
        this.User.write(out);
        this.ID.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.User.readFields(in);
        this.ID.readFields(in);
    }

    public int compareTo(UserAndIdWritable second){
        return this.User.compareTo(second.User);
    }

    public boolean equals(Object o){
        if (!(o instanceof UserAndIdWritable)) {
            return false;
        }
        UserAndIdWritable other = (UserAndIdWritable)o;
        if (this.User.equals(other.User) && this.ID.equals(other.ID)){
            return true;
        }
        else {
            return false;
        }
    }

    public int hashCode(){
        return this.User.hashCode();
    }
}
