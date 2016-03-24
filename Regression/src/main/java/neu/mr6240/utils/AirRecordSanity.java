package neu.mr6240.utils;

/**
 * This class is used for the sanity check of record
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class AirRecordSanity {

	private static final int CANCELLED = 47; 
	private static final int CRS_ARR_TIME = 40; 
	private static final int CRS_ELAPSED_TIME = 50; 
	private static final int CRS_DEP_TIME = 29; 
	private static final int ARR_TIME = 41;
	private static final int DEP_TIME = 30;
	private static final int ACTUAL_ELAPSED_TIME = 51; 
	private static final int ORIGIN_AIRPORT_ID = 11; 
	private static final int ORIGIN_AIRPORT_SEQ_ID = 12; 
	private static final int ORIGIN_CITY_MARKET_ID = 13; 
	private static final int ORIGIN_STATE_FIPS = 17; 
	private static final int ORIGIN_WAC = 19; 
	private static final int DEST_AIRPORT_ID = 20; 
	private static final int DEST_AIRPORT_SEQ_ID = 21; 
	private static final int DEST_CITY_MARKET_ID = 22; 
	private static final int DEST_STATE_FIPS = 26; 
	private static final int DEST_WAC = 28; 
	private static final int ORIGIN = 14; 
	private static final int DEST = 23; 
	private static final int ORIGIN_CITY_NAME = 15; 
	private static final int DEST_CITY_NAME = 24; 
	private static final int ORIGIN_STATE_NM = 18; 
	private static final int ORIGIN_STATE_ABR = 16; 
	private static final int ARR_DELAY = 42; 
	private static final int ARR_DELAY_NEW = 43; 
	private static final int ARR_DEL15 = 44;
	
	private static final int HOUR = 60;
	
	/**
	 * This method checks if the record passes all the sanity tests
	 * @param record
	 * @return boolean This returns true if the flight/record fails the sanity test
	 */
	public boolean sanityFails(String[] record) {
		return (checkCRSARRAndDEPTime(record) || 
				checkTimeZone(record) || 
				checkOriginAndDestForZero(record) || 
				checkOriginAndDestForEmpty(record) || 
				checkCancellation(record));
	}
	
	/**
	 * Sanity test for: CRSArrTime and CRSDepTime should not be zero
	 * @param record
	 * @return boolean This returns true if CRSArrTime and CRSDepTime is zero
	 */
	private boolean checkCRSARRAndDEPTime(String[] record) {
		return (Integer.parseInt(record[CRS_ARR_TIME]) == 0 || 
				Integer.parseInt(record[CRS_DEP_TIME]) == 0);
	}
	
	/**
	 * Sanity test for: timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime
	 * timeZone % 60 should be 0
	 * @param record
	 * @return boolean This returns true if timezone%60 is not zero
	 */
	private boolean checkTimeZone(String[] record) {
		try {
			return (getTimeZone(record[CRS_DEP_TIME], record[CRS_ARR_TIME], record[CRS_ELAPSED_TIME])%HOUR != 0); 
		}
		catch(NumberFormatException e) {
			return true;
		}
	}
	
	/**
	 * Get TimeZone
	 * @param record
	 * @return
	 */
	private int getTimeZone(String depTime, String arrTime, String elapsedTime) {
		int deptHour = Integer.valueOf(depTime.substring(0,2));
		int arrHour = Integer.valueOf(arrTime.substring(0,2));
		int deptMin = Integer.valueOf(depTime.substring(2,4));
		int arrMin = Integer.valueOf(arrTime.substring(2,4));
		
		int minDiff = (arrMin - deptMin);
		int hourDiff = 0;
		if((arrHour - deptHour) > 0)
			hourDiff = arrHour - deptHour;
		else if((arrHour - deptHour) < 0)
			hourDiff = arrHour - deptHour + 24;
		else {
			if(arrMin > deptMin) 
				hourDiff = arrHour - deptHour;
			else
				hourDiff = 24 - deptHour + arrHour;
		}
		
		int totalDiff = hourDiff * HOUR + minDiff;
		return (totalDiff - Integer.parseInt(elapsedTime)); 
	}
	
	/**
	 * Sanity test for: AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
	 * @param record
	 * @return boolean This returns true if AirportID,  AirportSeqID, CityMarketID, StateFips, Wac is lesser than or equal to 0
	 */
	private boolean checkOriginAndDestForZero(String[] record) {
		return (Integer.parseInt(record[ORIGIN_AIRPORT_ID]) <= 0 || 
				Integer.parseInt(record[ORIGIN_AIRPORT_SEQ_ID]) <= 0 || 
				Integer.parseInt(record[ORIGIN_CITY_MARKET_ID]) <= 0 || 
				Integer.parseInt(record[ORIGIN_STATE_FIPS]) <= 0 ||
				Integer.parseInt(record[ORIGIN_WAC]) <= 0 || 
				Integer.parseInt(record[DEST_AIRPORT_ID]) <= 0 || 
				Integer.parseInt(record[DEST_AIRPORT_SEQ_ID]) <= 0 || 
				Integer.parseInt(record[DEST_CITY_MARKET_ID]) <= 0 || 
				Integer.parseInt(record[DEST_STATE_FIPS]) <= 0 ||
				Integer.parseInt(record[DEST_WAC]) <= 0);
	}
	
	/**
	 * Sanity test for: Origin, Destination,  CityName, State, StateName should not be empty
	 * @param record
	 * @return boolean This returns true if Origin, Destination,  CityName, State, StateName is empty
	 */
	private boolean checkOriginAndDestForEmpty(String[] record) {
		return (record[ORIGIN] == null || record[ORIGIN].isEmpty() || 
				record[DEST] == null || record[DEST].isEmpty() ||
				record[ORIGIN_CITY_NAME] == null || record[ORIGIN_CITY_NAME].isEmpty() ||
				record[DEST_CITY_NAME] == null || record[DEST_CITY_NAME].isEmpty() ||
				record[ORIGIN_STATE_NM] == null || record[ORIGIN_STATE_NM].isEmpty() || 
				record[ORIGIN_STATE_ABR] == null || record[ORIGIN_STATE_ABR].isEmpty());
	}
	
	/**
	 * Sanity test for: For flights that not Cancelled:
	 * ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
	 * if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
	 * if ArrDelay < 0 then ArrDelayMinutes should be zero
	 * if ArrDelayMinutes >= 15 then ArrDel15 should be true
	 * @param record
	 * @return boolean This returns true if the flight was not Cancelled and
	 * ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
	 * if ArrDelay > 0 but ArrDelay is not equal to ArrDelayMinutes
	 * if ArrDelay < 0 but ArrDelayMinutes is not zero
	 * if ArrDelayMinutes >= 15 false ArrDel15 is false
	 */
	private boolean checkCancellation(String[] record) {
		if(Integer.parseInt(record[CANCELLED]) == 1)
			return false;
		
		return (checkTimeZoneDiff(record) || checkArrDelay(record));
	} 
	
	/**
	 * Sanity test for: ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
	 * @param record
	 * @return boolean This method returns true if ArrTime -  DepTime - ActualElapsedTime - timeZone is not equal to zero
	 */
	private boolean checkTimeZoneDiff(String[] record) {
		
		int timeZone = getTimeZone(record[CRS_DEP_TIME], record[CRS_ARR_TIME], record[CRS_ELAPSED_TIME]);		
		int arrDepElapsedDiff = getTimeZone(record[DEP_TIME], record[ARR_TIME], record[ACTUAL_ELAPSED_TIME]);
		
		int diff = arrDepElapsedDiff - timeZone;
		
		return (diff != 0);
	}
	
	/**
	 * Sanity test for:
	 * if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
	 * if ArrDelay < 0 then ArrDelayMinutes should be zero
	 * if ArrDelayMinutes >= 15 then ArrDel15 should be true
	 * @param record
	 * @return boolean This method returns true
	 * if ArrDelay > 0 then ArrDelay is not equal to ArrDelayMinutes
	 * if ArrDelay < 0 then ArrDelayMinutes is not zero
	 * if ArrDelayMinutes >= 15 then ArrDel15 is false
	 */
	private boolean checkArrDelay(String[] record) {
		if(Double.parseDouble(record[ARR_DELAY]) > 0) {
			if(Double.parseDouble(record[ARR_DELAY]) == Double.parseDouble(record[ARR_DELAY_NEW])) {
				if(Double.parseDouble(record[ARR_DELAY_NEW]) >= 15) {
					if(Double.parseDouble(record[ARR_DEL15]) == 1) {
						return false;
					}
				}
				else {
					return false;
				}
			}				
		}
		
		else {
			if(Double.parseDouble(record[ARR_DELAY_NEW]) == 0) {
				return false;
			}
		}
		return true;
	}
}
