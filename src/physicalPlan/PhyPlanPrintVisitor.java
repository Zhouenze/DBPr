package physicalPlan;

/*
 * A visitor to print a physical plan.
 * 
 * @author Enze Zhou ez242
 */
public final class PhyPlanPrintVisitor implements PhyOpVisitor {
	
	private String result = "";		// Print result
	
	/*
	 * Method to print a physical plan.
	 * @param phyPlan
	 * 		The plan that is being printed.
	 */
	public String printPhyPlan(PhyPlan phyPlan) {
		result = "Query:\t" + phyPlan.query + "\n";
		
		// Use visitor pattern to further print this plan.
		phyPlan.root.accept(this);
		return result;
	}

	/*
	 * Use visitor pattern to print a distinct physical operator.
	 * @override from interface PhyOpVisitor
	 * @param phyDistOp
	 * 		Distinct physical operator that is being printed.
	 */
	@Override
	public void visit(PhyDistOp phyDistOp) {
		result += "Dist:\t" + phyDistOp.hasOrderby + " " + phyDistOp.schema.toString() + "\n";
		phyDistOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to print a join physical operator.
	 * @override from interface PhyOpVisitor
	 * @param phyJoinOp
	 * 		Join physical operator that is being printed.
	 */
	@Override
	public void visit(PhyJoinOp phyJoinOp) {
		result += ("Join:\t" + phyJoinOp.schema + "\n");
		phyJoinOp.child.accept(this);
		phyJoinOp.rChild.accept(this);
		result += ("\tOn Join Conditions: " + phyJoinOp.conditions.toString() + "\n");
	}

	/*
	 * Use visitor pattern to print a projection physical operator.
	 * @override from interface PhyOpVisitor
	 * @param phyProjOp
	 * 		Projection physical operator that is being printed.
	 */
	@Override
	public void visit(PhyProjOp phyProjOp) {
		result += ("Proj:\t" + phyProjOp.selectAll + " " + phyProjOp.projAttrs.toString() + "\n");
		result += ("\tSchema: " + phyProjOp.schema.toString() + "\n");
		phyProjOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to print a scan physical operator.
	 * @override from interface PhyOpVisitor
	 * @param phyScanOp
	 * 		Scan physical operator that is being printed.
	 */
	@Override
	public void visit(PhyScanOp phyScanOp) {
		result += ("Scan:\t" + phyScanOp.alias + " is " + phyScanOp.fileName + "\tSchema: " + phyScanOp.schema.toString() + "\n");
		result += ("\tOn Scan Conditions: " + phyScanOp.conditions.toString() + "\n");
	}

	/*
	 * Use visitor pattern to print a sort physical operator.
	 * @override from interface PhyOpVisitor
	 * @param phySortOp
	 * 		Sort physical operator that is being printed.
	 */
	@Override
	public void visit(PhySortOp phySortOp) {
		result += ("Sort:\t" + phySortOp.sortAttrs.toString() + "\n");
		result += ("\tSchema: " + phySortOp.schema.toString() + "\n");
		phySortOp.child.accept(this);
	}
	
	/*
	 * Use visitor pattern to print a general physical operator. Mainly distribute.
	 * @override from interface PhyOpVisitor
	 * @param phyOp
	 * 		General physical operator that is being printed.
	 */
	@Override
	public void visit(PhyOp phyOp) {
		if (phyOp instanceof PhyDistOp)
			visit((PhyDistOp) phyOp);
		else if (phyOp instanceof PhyJoinOp)
			visit((PhyJoinOp) phyOp);
		else if (phyOp instanceof PhyProjOp)
			visit((PhyProjOp) phyOp);
		else if (phyOp instanceof PhyScanOp)
			visit((PhyScanOp) phyOp);
		else if (phyOp instanceof PhySortOp)
			visit((PhySortOp) phyOp);
	}

}
