package neu.mr6240.mapreduce.job1;

import java.io.IOException;

import neu.mr6240.utils.AirRecordSanity;
import neu.mr6240.utils.job1.CarrierYear;
import neu.mr6240.utils.job1.PriceTime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

/**
 * This is the Mapper class for the MapReduce program
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class LRMapper extends Mapper<LongWritable, Text, CarrierYear, PriceTime> {
	
	private static final int AVG_TICKET_PRICE = 109;
	private static final int CARRIER = 6;
	private static final int YEAR = 0;
	private static final int CRS_ELAPSED_TIME = 50;
	
	private static final int NUMBER_OF_COLUMNS = 110;
	
	private CSVParser parser = new CSVParser();
	private AirRecordSanity sanity = new AirRecordSanity();
	
	
	/**
	 * This method maps each row to a specific <Key,Value> which is the input to the reducer depending on the key
	 * It reads the input line by line and throws away the unnecessary information while generating the <Key, Value> pair
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String[] fields = parser.parseLine(value.toString());
		
		//Check if the row is not corrupt and the record is sane
		if(fields.length == NUMBER_OF_COLUMNS && !sanity.sanityFails(fields)) {		
			//Write <Key, Value> pair for the Reduce to take as the input
			context.write(new CarrierYear(fields[CARRIER], fields[YEAR]), new PriceTime(fields[AVG_TICKET_PRICE], fields[CRS_ELAPSED_TIME]));	
		}				
	}
	
}
