package neu.mr6240.mapreduce.job2;

import java.io.IOException;

import neu.mr6240.utils.AirRecordSanity;
import neu.mr6240.utils.job2.YearWeek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

/**
 * This is the Mapper class for the MapReduce program
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class WeekMapper extends Mapper<LongWritable, Text, YearWeek, IntWritable> {
	
	private static final int AVG_TICKET_PRICE = 109;
	private static final int CARRIER = 6;
	private static final int YEAR = 0;
	private static final int MONTH = 2;
	private static final int DAY_OF_MONTH = 3;
	private static final int NUMBER_OF_COLUMNS = 110;
	
	private static final String CHEAPEAST_CARRIER = "CheapestCarrier";
	private String cheapeastCarrier;
	
	private CSVParser parser = new CSVParser();
	private AirRecordSanity sanity = new AirRecordSanity();

	/**
	 * This method executes before the map method is executed
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		//Get the cheapest carrier computed in the previous mapreduce job
		cheapeastCarrier = context.getConfiguration().get(CHEAPEAST_CARRIER);        
    }
	
	/**
	 * This method maps each row to a specific <Key,Value> which is the input to the reducer depending on the key
	 * It reads the input line by line and throws away the unnecessary information while generating the <Key, Value> pair
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String[] fields = parser.parseLine(value.toString());
		
		//Check if the row is not corrupt, the record is sane and the carrier is the cheapest carrier
		if(fields.length == NUMBER_OF_COLUMNS && !sanity.sanityFails(fields) && fields[CARRIER].equals(cheapeastCarrier)) {		
			//Write <Key, Value> pair for the Reduce to take as the input
			context.write(new YearWeek(fields[YEAR], fields[DAY_OF_MONTH], fields[MONTH]), 
					new IntWritable((int) ((Double.valueOf(fields[AVG_TICKET_PRICE]) * 100))));	
		}				
	}

}
