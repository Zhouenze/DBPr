package physicalPlan;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;

import base.Condition;
import base.DBCatalog;
import logicalPlan.LogDistOp;
import logicalPlan.LogJoinOp;
import logicalPlan.LogOp;
import logicalPlan.LogOpVisitor;
import logicalPlan.LogPlan;
import logicalPlan.LogProjOp;
import logicalPlan.LogScanOp;
import logicalPlan.LogSortOp;

/*
 * Physical Plan class
 * This class is also a visitor to logical plan, so as to build a physical plan.
 * 
 * @author Enze Zhou ez242
 */
public class PhyPlan implements LogOpVisitor {

	String query = "";			// Original query.
	PhyOp temp = null;			// Used for building process.
	PhyCondOp dataRoot = null;	// Root node of the data part of this plan, maybe a join operator or a scan operator.
	LogPlan logPlan = null;		// Logical plan that is being translated.
	boolean r = false;			// Whether is building the right child of temp. Used for join tree building.
	
	
	public PhyOp root = null;	// Root node of this physical plan.
	
	int joinType, joinBuffer, sortType, sortBuffer;
	
	/*
	 * Another constructor of this class, ready for future usage.
	 * @param logPlan
	 * 		Logical plan that is being translated into a physical plan.
	 * @param config
	 * 		Configuration string that controls physical plan building.
	 */
	public PhyPlan(LogPlan logPlan) {
		query = logPlan.query;
		this.logPlan = logPlan;
		
		try {
			BufferedReader configReader = new BufferedReader(new FileReader(DBCatalog.getCatalog().inputPath + "plan_builder_config.txt"));
	
			String configLine = configReader.readLine();
			String [] columns = configLine.trim().split(" ");
			joinType = Integer.valueOf(columns[0]);
			if (joinType == 1)
				joinBuffer = Integer.valueOf(columns[1]);
			configLine = configReader.readLine();
			columns = configLine.trim().split(" ");
			sortType = Integer.valueOf(columns[0]);
			if (sortType == 1)
				sortBuffer = Integer.valueOf(columns[1]);
	
			configReader.close();
		} catch (Exception e) {
			System.err.println("Exception occurred when reading config file.");
			System.err.println(e.toString());
			e.printStackTrace();
		}
		
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
		
		if (joinType == 2 && dataRoot instanceof PhyJoinSMJOp)
			getSMJSortAttrs(dataRoot);
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
		((PhyDistBfOp)temp).hasOrderby = logDistOp.hasOrderby;
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
		PhyJoinOp joinOp = null;
		switch (joinType) {
		case 0:
		default:
			joinOp = new PhyJoinBfOp();
			if (logPlan.dataRoot == logJoinOp)
				dataRoot = joinOp;
			
			temp.child = joinOp;
			temp = joinOp;
			r = true;
			logJoinOp.rChild.accept(this);
			r = false;
			logJoinOp.child.accept(this);
			break;
		case 1:
			break;
		case 2:
			joinOp = new PhyJoinSMJOp();
			if (logPlan.dataRoot == logJoinOp)
				dataRoot = joinOp;
			
			temp.child = joinOp;
			
			joinOp.child = new PhySortExOp(sortBuffer);
			joinOp.rChild = new PhySortExOp(sortBuffer);
			
			r = false;
			temp = joinOp.rChild;
			logJoinOp.rChild.accept(this);
			temp = joinOp.child;
			logJoinOp.child.accept(this);
			
			break;
		}
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
		PhySortOp sortOp = null;
		switch (sortType) {
		case 0:
		default:
			sortOp = new PhySortBfOp();
			break;
		case 1:
			sortOp = new PhySortExOp(sortBuffer);
			break;
		}
		
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
		if (op instanceof PhyJoinOp && conditionDis(((PhyJoinOp)op).rChild, cond))
			return true;
		
		// If not enough information available in this node, return false.
		if (cond.leftName != null && op.schema.get(cond.leftName) == null)
			return false;
		if (cond.rightName != null && op.schema.get(cond.rightName) == null)
			return false;
		
		// Attach the condition to this node.
		if (op instanceof PhyJoinOp && op.child.schema.keySet().contains(cond.rightName))
			cond.flip();
		PhyCondOp condOp = (PhyCondOp)op;
		condOp.conditions.add(cond);
		return true;
	}
	
	void getSMJSortAttrs(PhyOp op) {
		if (op == null)
			return;
		if (op instanceof PhyJoinSMJOp) {
			PhyJoinSMJOp smjOp = (PhyJoinSMJOp)op;
			PhySortOp smjL = (PhySortOp)smjOp.child;
			PhySortOp smjR = (PhySortOp)smjOp.rChild;
			for (Condition cond : smjOp.conditions) {
				if (!smjL.sortAttrs.contains(cond.leftName))
					smjL.sortAttrs.add(cond.leftName);
				if (!smjR.sortAttrs.contains(cond.rightName))
					smjR.sortAttrs.add(cond.rightName);
			}
		}
		getSMJSortAttrs(op.child);
	}
}
