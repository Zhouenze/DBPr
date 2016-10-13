
public class PhyPlan {

	String query = "";
	PhyOp root = null;
	
	public PhyPlan(LogPlan logPlan, String config) {
		query = logPlan.query;
	}
}
