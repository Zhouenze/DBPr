
public class PhyPlanPrintVisitor implements PhyOpVisitor {
	
	String result = "";
	
	public String printPhyPlan(PhyPlan phyPlan) {
		result = "Query:\t" + phyPlan.query + "\n";
		phyPlan.root.accept(this);
		return result;
	}

	@Override
	public void visit(PhyDistOp phyDistOp) {
		result += "Dist\n";
		phyDistOp.child.accept(this);
	}

	@Override
	public void visit(PhyJoinOp phyJoinOp) {
		result += ("Join:\t" + phyJoinOp.schema + "\n");
		phyJoinOp.child.accept(this);
		phyJoinOp.rChild.accept(this);
		result += ("\tOn Join Conditions: " + phyJoinOp.conditions.toString());
	}

	@Override
	public void visit(PhyProjOp phyProjOp) {
		result += ("Proj:\t" + phyProjOp.selectAll + phyProjOp.projNames.toString() + "\n");
		result += ("\tSchema: " + phyProjOp.schema.toString() + "\n");
		phyProjOp.child.accept(this);
	}

	@Override
	public void visit(PhyScanOp phyScanOp) {
		result += ("Scan:\t" + phyScanOp.alias + " is " + phyScanOp.fileName + "\tSchema: " + phyScanOp.schema.toString() + "\n");
		result += ("\tOn Scan Conditions: " + phyScanOp.conditions.toString() + "\n");
	}

	@Override
	public void visit(PhySortOp phySortOp) {
		result += ("Sort:\t" + phySortOp.orderAttrs.toString() + "\n");
		result += ("\tSchema: " + phySortOp.schema.toString() + "\n");
		phySortOp.child.accept(this);
	}

}
