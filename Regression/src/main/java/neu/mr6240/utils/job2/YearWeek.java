package neu.mr6240.utils.job2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/**
 * This is class for a custom key for the map output key and hence the reduce input key  
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class YearWeek implements WritableComparable<YearWeek>{
	
	private static int MONTH_OFFSET = 1;
	
	private IntWritable year;
	private IntWritable week;
	
	public YearWeek(String year, String dayOfMonth, String month) {
		Integer yearInt = Integer.valueOf(year);
		Integer monthInt = Integer.valueOf(month) - MONTH_OFFSET;
		Integer dayOfMonthInt = Integer.valueOf(dayOfMonth);
		
		Calendar cal = Calendar.getInstance();
	    cal.set(yearInt, monthInt, dayOfMonthInt);
	    cal.setMinimalDaysInFirstWeek(1);
	    int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);

		set(new IntWritable(yearInt), new IntWritable(weekOfYear));
	} 
	
	public YearWeek() {
		set(new IntWritable(), new IntWritable());
	}
	
	public void set(IntWritable year, IntWritable week) {
		this.year = year;
		this.week = week;
	}
	
	@Override
	public String toString() {
		return year + "\t" + week;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		week.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		year.write(out);
		week.write(out);
	}
	
	@Override
	public int compareTo(YearWeek yearWeek) {
		return ComparisonChain.start().
				compare(year, yearWeek.year).
				compare(week, yearWeek.week).
				result();
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getWeek() {
		return week;
	}

	public void setWeek(IntWritable week) {
		this.week = week;
	}

}
