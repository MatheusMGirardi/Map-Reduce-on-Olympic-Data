package medium.consulta2_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryYearKey implements WritableComparable<CountryYearKey> {
    private Text country;
    private IntWritable year;

    public CountryYearKey() {
        this.country = new Text();
        this.year = new IntWritable();
    }

    public CountryYearKey(String country, int year) {
        this.country = new Text(country);
        this.year = new IntWritable(year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        country.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        year.readFields(in);
    }

    @Override
    public int compareTo(CountryYearKey o) {
        int cmp = country.compareTo(o.country);
        if (cmp != 0) {
            return cmp;
        }
        return year.compareTo(o.year);
    }

    public Text getCountry() {
        return country;
    }

    public IntWritable getYear() {
        return year;
    }

    public void set(String country, int year) {
        this.country.set(country);
        this.year.set(year);
    }

    @Override
    public String toString() {
        return "CountryYearKey{" +
                "country=" + country +
                ", year=" + year +
                '}';
    }
}