package neu.mr6240.utils.job1;

/**
 * This class is used to create a object having 2 fields, carrier and estimatedPrice, which is used as a value in map in the reducer phase
 * of mapreduce  
 * @author prasadmemane
 * @author swapnilmahajan
 */
public class CarrierEstPrice {
	
	private String carrier;
	private double estimatedPrice;
	
	public CarrierEstPrice(String carrier, double estimatedPrice) {
		this.carrier = carrier;
		this.estimatedPrice = estimatedPrice;
	}

	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}

	public double getEstimatedPrice() {
		return estimatedPrice;
	}

	public void setEstimatedPrice(double estimatedPrice) {
		this.estimatedPrice = estimatedPrice;
	}
	
	@Override
    public boolean equals(Object o) {
        if (this == o) 
        	return true;
        
        if (!(o instanceof CarrierEstPrice)) 
        	return false;
        
        CarrierEstPrice cm = (CarrierEstPrice) o;
        
        return estimatedPrice == cm.estimatedPrice && carrier.equals(cm.carrier);
    }

    @Override
    public int hashCode() {
        return 31 * carrier.hashCode() + (int) estimatedPrice;        
    }	

}
