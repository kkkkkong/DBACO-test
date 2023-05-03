package cloud.workflowScheduling.setting;

import java.io.Serializable;
import java.util.Comparator;

// virtual machine, i.e., cloud service resource
public class VM implements Serializable{
	public static final double LAUNCH_TIME = 0;	
	public static final long NETWORK_SPEED = 20 * 1024*1024;
	
	public static final int TYPE_NO = 9;
	public static final double[] SPEEDS = {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5};
	public static final double[] UNIT_COSTS = {0.12, 0.195, 0.28, 0.375, 0.48, 0.595, 0.72, 0.855, 1};
	public static final double INTERVAL = 3600;	//one hour, billing interval

	public static final int FASTEST = 8;
	public static final int SLOWEST = 0;
	
	private static int internalId = 0;
	public static void resetInternalId(){	//called by the constructor of Solution
		internalId = 0;
	}
	public static void setInternalId(int startId){
		internalId = startId;
	}
	
	
	private int id;
	private int type; 
	//GRP-HEFT
	private double effRate;

	public VM(int type){
		this.type = type;
		this.id = internalId++;
	}
	
	//------------------------getters && setters---------------------------
	void setType(int type) {		//can only be invoked in the same package, e.g., Solution
		this.type = type;
	}
	public double getSpeed(){		return SPEEDS[type];	}
	public double getUnitCost(){		return UNIT_COSTS[type];	}
	public int getId() {		return id;	}
	public int getType() {		return type;	}
	public void setEffRate(double effRate) {	
		this.effRate = effRate;
	}
	public double getEffRate() {	
		return this.effRate;
	}
	
	//-------------------------------------overrides--------------------------------
	public String toString() {
		return "VM [id=" + id + ", type=" + type + "]";
	}
	
	public static class EffRateComparator implements Comparator<VM>{
		public int compare(VM o1, VM o2) {
			if(o1.getEffRate() > o2.getEffRate())
				return 1;
			else if(o1.getEffRate() < o2.getEffRate())
				return -1;
			else{
				if(o1.getSpeed() > o2.getSpeed())
					return 1;
				else if(o1.getSpeed() < o2.getSpeed())
					return -1;
				else
					return 0;
			}
		}
	}
}