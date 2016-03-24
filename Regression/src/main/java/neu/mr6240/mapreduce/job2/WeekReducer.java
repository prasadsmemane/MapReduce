package neu.mr6240.mapreduce.job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import neu.mr6240.utils.job2.YearWeek;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is a reducer class which computes the median price per week for the cheapest carrier
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class WeekReducer extends Reducer<YearWeek, IntWritable, YearWeek, DoubleWritable> {
	
	public static final double TO_ROUND_TO_2_DECIMALS = 100.0;
	public static final int HALF = 2;
	public static final int ODD_COUNT = 1;
	
	/**
	 * This method takes the <Key,Iterable<Value>> method from the Mapper output and generates a <Key, Value> output 
	 */
	@Override
	protected void reduce(YearWeek key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		List<Integer> listOfPrice = new ArrayList<Integer>();
		for(IntWritable value : values)
			listOfPrice.add(value.get());
		
		context.write(key, new DoubleWritable(getMedian(listOfPrice)));
	}
	
	/**
	 * This method computes the median for the given list of prices
	 * @param listOfPrice
	 * @return
	 */
	private double getMedian(List<Integer> listOfPrice) {
		Collections.sort(listOfPrice);
		int middle = listOfPrice.size()/HALF;
		if (listOfPrice.size()%HALF == ODD_COUNT) {
			return listOfPrice.get(middle) / TO_ROUND_TO_2_DECIMALS;
		} else {
			return (listOfPrice.get(middle-ODD_COUNT) + listOfPrice.get(middle)) / (TO_ROUND_TO_2_DECIMALS * HALF);
		}
	}

}
