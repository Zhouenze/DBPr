
/*
 * A visitor to print a logical plan.
 * 
 * @author Enze Zhou ez242
 */
public class LogPlanPrintVisitor implements LogOpVisitor {
	
	String result = "";		// Print result
	
	/*
	 * Method to print a logical plan.
	 * @param logPlan
	 * 		The plan that is being printed.
	 */
	public String printLogPlan(LogPlan logPlan) {
		
		// Print basic information of this plan.
		result = "Query:\t" + logPlan.query + "\n";
		result += ("Alias Dict:\t" + logPlan.aliasDict.toString() + "\n");
		result += ("Conditions:\t" + logPlan.conditions.toString() + "\n");
		result += ("NaiveJoinOrder:\t" + logPlan.naiveJoinOrder.toString() + "\n");
		
		// Use visitor pattern to further print this plan.
		logPlan.root.accept(this);
		return result;
	}

	/*
	 * Use visitor pattern to print a distinct logical operator.
	 * @override from interface LogOpVisitor
	 * @param logDistOp
	 * 		Distinct logical operator that is being printed.
	 */
	@Override
	public void visit(LogDistOp logDistOp) {
		result += "Dist\n";
		logDistOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to print a join logical operator.
	 * @override from interface LogOpVisitor
	 * @param logJoinOp
	 * 		Join logical operator that is being printed.
	 */
	@Override
	public void visit(LogJoinOp logJoinOp) {
		result += "Join\n";
		logJoinOp.child.accept(this);
		logJoinOp.rChild.accept(this);
	}

	/*
	 * Use visitor pattern to print a projection logical operator.
	 * @override from interface LogOpVisitor
	 * @param logProjOp
	 * 		Projection logical operator that is being printed.
	 */
	@Override
	public void visit(LogProjOp logProjOp) {
		result += ("Proj:\t" + logProjOp.selectAll + " " + logProjOp.projAttrs.toString() + "\n");
		logProjOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to print a scan logical operator.
	 * @override from interface LogOpVisitor
	 * @param logScanOp
	 * 		Scan logical operator that is being printed.
	 */
	@Override
	public void visit(LogScanOp logScanOp) {
		result += "Scan\n";
	}

	/*
	 * Use visitor pattern to print a sort logical operator.
	 * @override from interface LogOpVisitor
	 * @param logSortOp
	 * 		Sort logical operator that is being printed.
	 */
	@Override
	public void visit(LogSortOp logSortOp) {
		result += ("Sort:\t" + logSortOp.sortAttrs.toString() + '\n');
		logSortOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to print a general logical operator. Mainly distribute.
	 * @override from interface LogOpVisitor
	 * @param logOp
	 * 		General logical operator that is being printed.
	 */
	@Override
	public void visit(LogOp logOp) {
		if (logOp instanceof LogDistOp)
			visit((LogDistOp) logOp);
		else if (logOp instanceof LogJoinOp)
			visit((LogJoinOp) logOp);
		else if (logOp instanceof LogProjOp)
			visit((LogProjOp) logOp);
		else if (logOp instanceof LogScanOp)
			visit((LogScanOp) logOp);
		else if (logOp instanceof LogSortOp)
			visit((LogSortOp) logOp);
	}

}
