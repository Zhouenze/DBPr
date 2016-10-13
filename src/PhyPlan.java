
/*
 * Physical Plan class
 * This class is also a visitor to logical plan, so as to build a physical plan.
 * 
 * @author Enze Zhou ez242
 */
public class PhyPlan implements LogOpVisitor {

	String query = "";			// Original query.
	PhyOp root = null;			// Root node of this physical plan.
	PhyOp temp = null;			// Used for building process.
	PhyCondOp dataRoot = null;	// Root node of the data part of this plan, maybe a join operator or a scan operator.
	LogPlan logPlan = null;		// Logical plan that is being translated.
	boolean r = false;			// Whether is building the right child of temp. Used for join tree building.
	
	/*
	 * Constructor of this class.
	 * Copy original query and use visitor pattern to build plan.
	 * @param logPlan
	 * 		Logical plan that is being translated into a physical plan.
	 */
	public PhyPlan(LogPlan logPlan) {
		// Copy some basic information
		query = logPlan.query;
		this.logPlan = logPlan;
		
		// Use visitor pattern to build physical plan.
		logPlan.root.accept(this);
		
		// Build schema of each node.
		root.buildSchema();
		
		// Distribute conditions of this query.
		for (Condition cond : logPlan.conditions) {
			if (cond.leftName == null && cond.rightName == null)
				dataRoot.conditions.add(cond);
			else
				if (!conditionDis(dataRoot, cond))
					System.err.println("Condition undispensed!");
		}
	}
	
	/*
	 * Another constructor of this class, ready for future usage.
	 * @param logPlan
	 * 		Logical plan that is being translated into a physical plan.
	 * @param config
	 * 		Configuration string that controls physical plan building.
	 */
	public PhyPlan(LogPlan logPlan, String config) {
		query = logPlan.query;
		this.logPlan = logPlan;
	}

	/*
	 * Use visitor pattern to build physical operator from logical operator
	 * @override from LogOpVisitor interface
	 * @param logDistOp
	 * 		Distinct logical operator to be translated.
	 */
	@Override
	public void visit(LogDistOp logDistOp) {
		root = temp = new PhyDistBfOp();
		logDistOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to build physical operator from logical operator
	 * @override from LogOpVisitor interface
	 * @param logJoinOp
	 * 		Join logical operator to be translated.
	 */
	@Override
	public void visit(LogJoinOp logJoinOp) {
		PhyJoinOp joinOp = new PhyJoinBfOp();
		if (logPlan.dataRoot == logJoinOp)
			dataRoot = joinOp;
		temp.child = joinOp;
		temp = joinOp;
		r = true;
		logJoinOp.rChild.accept(this);
		r = false;
		logJoinOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to build physical operator from logical operator
	 * @override from LogOpVisitor interface
	 * @param logProjOp
	 * 		Projection logical operator to be translated.
	 */
	@Override
	public void visit(LogProjOp logProjOp) {
		PhyProjBfOp projOp = new PhyProjBfOp();
		projOp.projAttrs = logProjOp.projAttrs;
		projOp.selectAll = logProjOp.selectAll;
		if (temp == null) {
			root = temp = projOp;
		} else {
			temp.child = projOp;
			temp = projOp;
		}
		logProjOp.child.accept(this);
	}

	/*
	 * Use visitor pattern to build physical operator from logical operator
	 * @override from LogOpVisitor interface
	 * @param logScanOp
	 * 		Scan logical operator to be translated.
	 */
	@Override
	public void visit(LogScanOp logScanOp) {
		PhyScanOp scanOp = new PhyScanBfOp();
		if (logPlan.dataRoot == logScanOp)
			dataRoot = scanOp;
		scanOp.alias = logPlan.naiveJoinOrder.remove(logPlan.naiveJoinOrder.size() - 1);
		scanOp.fileName = logPlan.aliasDict.get(scanOp.alias);
		if (r) {
			((PhyJoinOp)temp).rChild = scanOp;
		} else {
			temp.child = scanOp;
		}
	}

	/*
	 * Use visitor pattern to build physical operator from logical operator
	 * @override from LogOpVisitor interface
	 * @param logSortOp
	 * 		Sort logical operator to be translated.
	 */
	@Override
	public void visit(LogSortOp logSortOp) {
		PhySortOp sortOp = new PhySortBfOp();
		sortOp.sortAttrs = logSortOp.sortAttrs;
		if (temp == null) {
			root = temp = sortOp;
		} else {
			temp.child = sortOp;
			temp = sortOp;
		}
		logSortOp.child.accept(this);
	}
	
	/*
	 * Use visitor pattern to build physical operator from logical operator. Mainly distribute.
	 * @override from LogOpVisitor interface
	 * @param logOp
	 * 		General logical operator to be translated.
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
	
	/*
	 * Condition dispenser
	 * @param op
	 * 		Check whether this condition should be attached to this node.
	 * @param cond
	 * 		The condition that is being attached.
	 * @return whether the condition is successfully attached.
	 */
	boolean conditionDis(PhyOp op, Condition cond) {
		if (op == null)
			return false;
		
		// If successfully attached to a child node, shortcut return.
		if (conditionDis(op.child, cond))
			return true;
		if (op instanceof PhyJoinBfOp && conditionDis(((PhyJoinBfOp)op).rChild, cond))
			return true;
		
		// If not enough information available in this node, return false.
		if (cond.leftName != null && op.schema.get(cond.leftName) == null)
			return false;
		if (cond.rightName != null && op.schema.get(cond.rightName) == null)
			return false;
		
		// Attach the condition to this node.
		PhyCondOp condOp = (PhyCondOp)op;
		condOp.conditions.add(cond);
		return true;
	}
}
