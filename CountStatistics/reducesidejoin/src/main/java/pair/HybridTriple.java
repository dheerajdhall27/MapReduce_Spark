package pair;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HybridTriple implements WritableComparable<HybridTriple> {

    private int first;
    private int second;
    private char third;

    public HybridTriple() {
    }

    public HybridTriple(int first, int second, char ch) {
        set(first, second, ch);
    }

    public void set(int first, int second, char ch) {
        this.first = first;
        this.second = second;
        this.third = ch;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public char getThird() {
        return third;
    }

    @Override
    public int compareTo(HybridTriple hybridTriple) {
        int cmp = compare(first, hybridTriple.first);
        if (cmp != 0) {
            return cmp;
        }

        return compare(second, hybridTriple.second);
    }

    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first);
        dataOutput.writeInt(this.second);
        dataOutput.writeChar(this.third);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
        third = dataInput.readChar();
    }

    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof HybridTriple) {
            HybridTriple ip = (HybridTriple) o;
            return first == ip.first && second == ip.second;
        }

        return false;
    }
}
