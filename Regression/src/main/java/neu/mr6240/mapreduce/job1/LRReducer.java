package neu.mr6240.mapreduce.job1;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import neu.mr6240.utils.job1.CarrierEstPrice;
import neu.mr6240.utils.job1.CarrierYear;
import neu.mr6240.utils.job1.PriceTime;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is a reducer class which finds the cheapest carrier
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class LRReducer extends Reducer<CarrierYear, PriceTime, Text, Text> {
	
	private static final String N = "scheduledFlightMins";
	private static final String CHEAPEAST_CARRIER = "CheapestCarrier";
	private static final String SEPARATOR = "=";
	private static final int INCREMENT = 1;
	public static final double TO_ROUND_TO_2_DECIMALS = 100.0;
	
	private Map<Integer, CarrierEstPrice> cheapeastCarrierMap = new HashMap<Integer, CarrierEstPrice>();
	
	/**
	 * This method takes the <Key,Iterable<Value>> method from the Mapper output and generates a <Key, Value> output 
	 */
	@Override
	protected void reduce(CarrierYear key, Iterable<PriceTime> values, Context context) throws IOException, InterruptedException {
		SimpleRegression sRegression = new SimpleRegression();
		
		//Add x=time, y=price values to the regression model
		for(PriceTime value : values)
			sRegression.addData(value.getTime().get(), value.getPrice().get());
		
		//Compute the estimated Price for the given value of time argument passed through command line
		double estimatedPrice = Math.round(sRegression.predict(Double.valueOf(context.getConfiguration().get(N))) 
				* TO_ROUND_TO_2_DECIMALS) / TO_ROUND_TO_2_DECIMALS;
		manipulateCheapeastCarrierMap(key, estimatedPrice);
	}
	
	/**
	 * This method runs after the reduce method is completed.
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		Map<String, Integer> winnerCountMap = new HashMap<String, Integer>();
		
		//Create the map which contains key as the carrier and value as the number of times the carrier was cheapest for a year
		createWinnerCountMap(winnerCountMap, cheapeastCarrierMap);
		
		//Get the cheapest carrier from the map which has the highest value
		String cheapeastCarrier = getCheapeastCarrier(winnerCountMap);		
		context.getCounter(CHEAPEAST_CARRIER, CHEAPEAST_CARRIER + SEPARATOR + cheapeastCarrier).increment(INCREMENT);	
		context.write(new Text(cheapeastCarrier), new Text());
	}
	
	/**
	 * This method adds a CarrierEstPrice object as a value and Year as the key to a map
	 * If the map does not contain the key, add a new CarrierEstPrice object with carrier and the estimated price fields
	 * If the map does contain the key, check if the existing CarrierEstPrice object for that Year key has estimated price
	 * field higher than the current estimated price, if yes, change the CarrierEstPrice object by setting the
	 * current carrier and current estimated price
	 * @param key
	 * @param estimatedPrice
	 */
	private void manipulateCheapeastCarrierMap(CarrierYear key, double estimatedPrice) {
		if(cheapeastCarrierMap.containsKey(key.getYear().get()))
			updateMap(key, estimatedPrice);
		else
			cheapeastCarrierMap.put(key.getYear().get(), new CarrierEstPrice(key.getCarrier().toString(), estimatedPrice));
	}
	
	/**
	 * This method checks if the existing CarrierEstPrice object for that Year key has estimated price
	 * field higher than the current estimated price, if yes, change the CarrierEstPrice object by setting the
	 * current carrier and current estimated price
	 * @param key
	 * @param estimatedPrice
	 */
	private void updateMap(CarrierYear key, double estimatedPrice) {
		if(cheapeastCarrierMap.get(key.getYear().get()).getEstimatedPrice() > estimatedPrice) {
			CarrierEstPrice cep = cheapeastCarrierMap.get(key.getYear().get());
			cep.setCarrier(key.getCarrier().toString());
			cep.setEstimatedPrice(estimatedPrice);
		}			
	}	
	
	/**
	 * This method creates a map, with carrier as the key and value as the number of times that carrier was
	 * cheapest in the year 
	 * @param winnerCountMap
	 * @param cheapeastCarrierMap
	 */
	private void createWinnerCountMap(Map<String, Integer> winnerCountMap, Map<Integer, CarrierEstPrice> cheapeastCarrierMap) {
		for(Map.Entry<Integer, CarrierEstPrice> entry : cheapeastCarrierMap.entrySet()) {
			if(winnerCountMap.containsKey(entry.getValue().getCarrier()))
				winnerCountMap.put(entry.getValue().getCarrier(), winnerCountMap.get(entry.getValue().getCarrier()) + INCREMENT);
			else
				winnerCountMap.put(entry.getValue().getCarrier(), INCREMENT);				
		}
	}
	
	/**
	 * This method finds the cheapest carrier from the winnerCountMap
	 * @param winnerCountMap
	 * @return
	 */
	private String getCheapeastCarrier(Map<String, Integer> winnerCountMap) {
		TreeMap<Integer, String> sortedWinnerMap = new TreeMap<Integer, String>(Collections.reverseOrder());
		for(Map.Entry<String, Integer> entry : winnerCountMap.entrySet())
			sortedWinnerMap.put(entry.getValue(), entry.getKey());
		
		return sortedWinnerMap.firstEntry().getValue();
	}
	
}
