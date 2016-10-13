
/*
 * Interface of logical visitor.
 * 
 * @author Enze Zhou ez242
 */
public interface LogOpVisitor {
	
	/*
	 * Method that is used to visit a general logical operator.
	 * Usually a method to distribute to other methods.
	 * @param logOp
	 * 		General logical operator that is being visited.
	 */
	void visit(LogOp logOp);
	
	/*
	 * Method that is used to visit a distinct logical operator.
	 * @param logDistOp
	 * 		Distinct logical operator that is being visited.
	 */
	void visit(LogDistOp logDistOp);
	
	/*
	 * Method that is used to visit a join logical operator.
	 * @param logJoinOp
	 * 		Join logical operator that is being visited.
	 */
	void visit(LogJoinOp logJoinOp);
	
	/*
	 * Method that is used to visit a projection logical operator.
	 * @param logProjOp
	 * 		Projection logical operator that is being visited.
	 */
	void visit(LogProjOp logProjOp);
	
	/*
	 * Method that is used to visit a scan logical operator.
	 * @param logScanOp
	 * 		Scan logical operator that is being visited.
	 */
	void visit(LogScanOp logScanOp);
	
	/*
	 * Method that is used to visit a sort logical operator.
	 * @param logSortOp
	 * 		Sort logical operator that is being visited.
	 */
	void visit(LogSortOp logSortOp);
}
