package neu.mr6240.utils.job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/**
 * This is class for a custom key for the map output key and hence the reduce input key  
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class CarrierYear implements WritableComparable<CarrierYear>{
	
	private Text carrier;
	private IntWritable year;
	
	public CarrierYear(Text carrier, IntWritable year) {
		set(carrier, year);
	}
	
	public CarrierYear(String carrier, String year) {
		set(new Text(carrier), new IntWritable(Integer.valueOf(year)));
	}
	
	public CarrierYear(String carrier, Integer year) {
		set(new Text(carrier), new IntWritable(year));
	}
	
	public CarrierYear() {
		set(new Text(), new IntWritable());
	}
	
	public void set(Text carrier, IntWritable year) {
		this.carrier = carrier;
		this.year = year;
	}

	@Override
	public String toString() {
		return carrier + "\t" + year;
	}

	public Text getCarrier() {
		return carrier;
	}

	public void setCarrier(Text carrier) {
		this.carrier = carrier;
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		carrier.readFields(in);
		year.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		carrier.write(out);
		year.write(out);		
	}

	@Override
	public int compareTo(CarrierYear carrierYear) {
		return ComparisonChain.start().
				compare(carrier, carrierYear.carrier).
				compare(year, carrierYear.year).
				result();
	}
	
}
