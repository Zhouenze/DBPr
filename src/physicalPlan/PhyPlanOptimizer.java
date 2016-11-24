package physicalPlan;

import java.util.HashMap;
import java.util.Vector;

import logicalPlan.LogPlan;

public class PhyPlanOptimizer {
	
	private LogPlan logPlan = null;
	
	public class scanInfo {
		public String fileName;
		public Integer type;
		public String keyName;
	}
	
	public HashMap<String, scanInfo> scanPlan = new HashMap<>();
	
	public class JoinPlan {
		public Vector<Integer> joinTypes = new Vector<>();
		public Vector<String> joinOrder = new Vector<>();
	}
	
	JoinPlan finalJoinPlan = null;
	
	public PhyPlanOptimizer(LogPlan logPlan) {
		this.logPlan = logPlan;
		
	}
}
