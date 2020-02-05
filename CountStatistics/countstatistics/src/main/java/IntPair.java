
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int first;
    private int second;

    public IntPair(){}

    public IntPair(int first, int second) {
        set(first, second);
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public int compareTo(IntPair intPair) {
        int cmp = compare(first, intPair.first);
        if (cmp != 0) {
            return cmp;
        }

        return compare(second, intPair.second);
    }

    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first);
        dataOutput.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IntPair) {
            IntPair ip = (IntPair) o;
            return first == ip.first && second == ip.second;
        }

        return false;
    }
}
