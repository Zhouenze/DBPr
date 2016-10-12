
public class LogPlanPrintVisitor implements LogOpVisitor {
	
	String result;
	
	public String printLogPlan(LogPlan logPlan) {
		result = "Query:\t" + logPlan.query + "\n";
		result += ("Alias Dict:\t" + logPlan.aliasDict.toString() + "\n");
		result += ("Conditions:\t" + logPlan.conditions.toString() + "\n");
		result += ("NaiveJoinOrder:\t" + logPlan.naiveJoinOrder.toString() + "\n");
		logPlan.root.accept(this);
		return result;
	}

	@Override
	public void visit(LogDistOp logDistOp) {
		result += "Dist\n";
		logDistOp.child.accept(this);
	}

	@Override
	public void visit(LogJoinOp logJoinOp) {
		result += "Join\n";
		logJoinOp.child.accept(this);
		logJoinOp.rChild.accept(this);
	}

	@Override
	public void visit(LogProjOp logProjOp) {
		result += ("Proj:\t" + logProjOp.selectAll + " " + logProjOp.projAttrs.toString() + "\n");
		logProjOp.child.accept(this);
	}

	@Override
	public void visit(LogScanOp logScanOp) {
		result += "Scan\n";
	}

	@Override
	public void visit(LogSortOp logSortOp) {
		result += ("Sort:\t" + logSortOp.sortAttrs.toString() + '\n');
		logSortOp.child.accept(this);
	}

}
