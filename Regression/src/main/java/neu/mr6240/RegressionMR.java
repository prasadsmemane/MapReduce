package neu.mr6240;
import java.io.IOException;

import neu.mr6240.mapreduce.job1.LRMapper;
import neu.mr6240.mapreduce.job1.LRReducer;
import neu.mr6240.mapreduce.job2.WeekMapper;
import neu.mr6240.mapreduce.job2.WeekReducer;
import neu.mr6240.utils.CommandLineParser;
import neu.mr6240.utils.job1.CarrierYear;
import neu.mr6240.utils.job1.PriceTime;
import neu.mr6240.utils.job2.YearWeek;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is the main class which runs the mapreduce
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class RegressionMR extends Configured implements Tool {
	
	private static final String N = "scheduledFlightMins";	
	private static final String CHEAPEAST_CARRIER = "CheapestCarrier";
	private static final String INTERMEDIATE = "intermediate";
	private static final String SEPERATOR = "=";

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RegressionMR(), args));
	}

	public int run(String[] args) throws Exception {
		CommandLineParser cmdParser = new CommandLineParser(args);

		//Setup MapReduce Job1 to get the cheapest carrier
		String chepeastCarrier = setupJob1(cmdParser);		
		//Setup MapReduce Job2 to compute the median price per week of the cheapest carrier
		return setupJob2(cmdParser, chepeastCarrier);
	}
		
	/**
	 * This method setups the mapreduce job1 which finds the cheapest carrier using linear regression
	 * @param cmdParser
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private String setupJob1(CommandLineParser cmdParser) throws IOException, ClassNotFoundException, InterruptedException {
		// Create new configuration
        Configuration conf = new Configuration();
		conf.set(N, cmdParser.getTime());
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(RegressionMR.class);
		
		job.setNumReduceTasks(1);
		
        // Setup MapReduce
        job.setMapperClass(LRMapper.class);
        job.setReducerClass(LRReducer.class);
                
        // Setup Mapper Output <Key,Value>
        job.setMapOutputKeyClass(CarrierYear.class);
        job.setMapOutputValueClass(PriceTime.class);
 
        // Specify Output <Key,Value>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        // Input
        FileInputFormat.addInputPath(job, new Path(cmdParser.getInputDir())); 
        // Output
        FileOutputFormat.setOutputPath(job, new Path(cmdParser.getOutputDir() + INTERMEDIATE));
        
        // Wait For the job to complete
        job.waitForCompletion(true);
        
        Counters counters = job.getCounters();
        
        String cheapeastCarrier = null;
        CounterGroup cGroup =  counters.getGroup(CHEAPEAST_CARRIER);
        for(Counter c : cGroup) {
        	if(c.getDisplayName().startsWith(CHEAPEAST_CARRIER)) {
        		cheapeastCarrier = c.getDisplayName().substring(c.getDisplayName().indexOf(SEPERATOR) + 1); 
        	}
        }
        return cheapeastCarrier;
	}
	
	/**
	 * This method setups the mapreduce job2 which computes the median price per week for the cheapest carrier 
	 * calculated by mapreduce job1
	 * @param cmdParser
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private int setupJob2(CommandLineParser cmdParser, String cheapeastCarrier) throws IOException, ClassNotFoundException, InterruptedException { 
		// Create new configuration
		Configuration conf2 = new Configuration();
        conf2.set(CHEAPEAST_CARRIER, cheapeastCarrier);
        
        Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(RegressionMR.class);
		
		job2.setNumReduceTasks(1);
		
        // Setup MapReduce
        job2.setMapperClass(WeekMapper.class);
        job2.setReducerClass(WeekReducer.class);
 
        // Setup Mapper Output <Key,Value>
        job2.setMapOutputKeyClass(YearWeek.class);
        job2.setMapOutputValueClass(IntWritable.class);
        
        // Specify Output <Key,Value>
        job2.setOutputKeyClass(YearWeek.class);
        job2.setOutputValueClass(DoubleWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job2, new Path(cmdParser.getInputDir())); 
        // Output
        FileOutputFormat.setOutputPath(job2, new Path(cmdParser.getOutputDir()));
 
        // Wait for job to complete and return status
        return job2.waitForCompletion(true) ? 0 : 1;
	}
}
