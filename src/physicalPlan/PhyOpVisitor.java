package physicalPlan;

/*
 * Interface of physical visitor.
 * 
 * @author Enze Zhou ez242
 */
public interface PhyOpVisitor {
	
	/*
	 * Method that is used to visit a general physical operator.
	 * Usually a method to distribute to other methods.
	 * @param phyOp
	 * 		General physical operator that is being visited.
	 */
	void visit(PhyOp phyOp);
	
	/*
	 * Method that is used to visit a distinct physical operator.
	 * @param phyDistOp
	 * 		Distinct physical operator that is being visited.
	 */
	void visit(PhyDistOp PhyDistOp);
	
	/*
	 * Method that is used to visit a join physical operator.
	 * @param phyJoinOp
	 * 		Join physical operator that is being visited.
	 */
	void visit(PhyJoinOp PhyJoinOp);
	
	/*
	 * Method that is used to visit a projection physical operator.
	 * @param phyProjOp
	 * 		Projection physical operator that is being visited.
	 */
	void visit(PhyProjOp PhyProjOp);
	
	/*
	 * Method that is used to visit a scan physical operator.
	 * @param phyScanOp
	 * 		Scan physical operator that is being visited.
	 */
	void visit(PhyScanOp PhyScanOp);
	
	/*
	 * Method that is used to visit a sort physical operator.
	 * @param phySortOp
	 * 		Sort physical operator that is being visited.
	 */
	void visit(PhySortOp PhySortOp);
}
