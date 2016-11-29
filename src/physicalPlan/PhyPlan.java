package physicalPlan;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map.Entry;

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
public final class PhyPlan implements LogOpVisitor {

	String query = "";					// Original query.
	private PhyOp temp = null;			// Used for building process.
	public PhyCondOp dataRoot = null;	// Root node of the data part of this plan, maybe a join operator or a scan operator.
	private LogPlan logPlan = null;		// Logical plan that is being translated.
	private boolean r = false;			// Whether is building the right child of temp. Used for join tree building.
	
	
	public PhyOp root = null;	// Root node of this physical plan.
	
	private int joinType, joinBuffer, sortType, sortBuffer, scanType;		// The configuration of this plan.
	
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
			
			// Set configuration according to config file.
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
			configLine = configReader.readLine();
			scanType = Integer.valueOf(configLine.trim());
	
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
		
		// The attributes to be sorted by under a sort-merge-join operator is set here.
		if (joinType == 2 && dataRoot instanceof PhyJoinSMJOp)
			getSMJSortAttrs(dataRoot);
		
		// Index scan operators need to be initialized here because they need to know their schema and build high-low value with conditions distributed above.
		if (scanType == 1) {
			initializeIndexScans(dataRoot);
		}
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
		case 0:										// Brute force join.
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
		case 1:										// Block nested loop join.
			joinOp = new PhyJoinBNLJOp(joinBuffer);
			if (logPlan.dataRoot == logJoinOp)
				dataRoot = joinOp;
			
			temp.child = joinOp;
			temp = joinOp;
			r = true;
			logJoinOp.rChild.accept(this);
			r = false;
			logJoinOp.child.accept(this);
			break;
		case 2:										// Sort-merge join.
			joinOp = new PhyJoinSMJOp();
			if (logPlan.dataRoot == logJoinOp)
				dataRoot = joinOp;
			
			temp.child = joinOp;
			
			switch (sortType) {
			case 0:
			default:
				joinOp.child = new PhySortBfOp();
				joinOp.rChild = new PhySortBfOp();
				break;
			case 1:
				joinOp.child = new PhySortExOp(sortBuffer);
				joinOp.rChild = new PhySortExOp(sortBuffer);
				break;
			}
			
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
		
		PhyScanOp scanOp = null;
		String alias = logPlan.naiveJoinOrder.remove(logPlan.naiveJoinOrder.size() - 1);
		String fileName = logPlan.aliasDict.get(alias);
		if (scanType == 1 && !DBCatalog.getCatalog().tables.get(fileName).indexes.isEmpty()) {
			scanOp = new PhyScanIndexOp(fileName, alias, null);
		} else {
			scanOp = new PhyScanBfOp();
			scanOp.alias = alias;
			scanOp.fileName = fileName;
		}
		
		if (logPlan.dataRoot == logScanOp)
			dataRoot = scanOp;
		if (r) {
			((PhyJoinOp)temp).rChild = scanOp;
		} else {
			temp.child = scanOp;
		}
		
		// Additional initialization for BNLJ
		if (temp instanceof PhyJoinBNLJOp) {
			String append = (DBCatalog.getCatalog().inputPath.contains("/") ? "db/data/" : "db\\data\\");
			((PhyJoinBNLJOp)temp).setLeftFile(	DBCatalog.getCatalog().tables.get(scanOp.fileName).attrs.size(), 
												DBCatalog.getCatalog().inputPath + append + scanOp.fileName);
		}
	}

	/*
	 * Use visitor pattern to build physical operator from logical operator
	 * @override from LogOpVisitor interface
	 * @param logSortOp
	 * 		Sort logical operator to be translated.
	 */
	@Override
	public void visit(LogSortOp logSortOp) {		// This function is only called for the top sort operator because there is only one logical sort, others are added by sort-merge-join.
		PhySortOp sortOp = null;
		switch (sortType) {
		case 0:							// Brute force
		default:
			sortOp = new PhySortBfOp();
			break;
		case 1:							// External sort
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
	private boolean conditionDis(PhyOp op, Condition cond) {
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
		// Modify condition to make sure that the first element corresponding to child and second to rChild. This assumption is necessary for smj. 
		if (op instanceof PhyJoinOp && op.child.schema.keySet().contains(cond.rightName))
			cond.flip();
		PhyCondOp condOp = (PhyCondOp)op;
		condOp.conditions.add(cond);
		return true;
	}
	
	/*
	 * This function sets the sort attributes of sort-merge-join operator children.
	 * @param
	 * 		op the operator that is trying to distribute sort attributes.
	 */
	private void getSMJSortAttrs(PhyOp op) {
		if (op == null)
			return;
		if (op instanceof PhyJoinSMJOp) {
			PhyJoinSMJOp smjOp = (PhyJoinSMJOp)op;
			PhySortOp smjL = (PhySortOp)smjOp.child;
			PhySortOp smjR = (PhySortOp)smjOp.rChild;
			for (Condition cond : smjOp.conditions) {
				if (!smjL.sortAttrs.contains(cond.leftName))		// No duplication allowed.
					smjL.sortAttrs.add(cond.leftName);
				if (!smjR.sortAttrs.contains(cond.rightName))
					smjR.sortAttrs.add(cond.rightName);
			}
		}
		getSMJSortAttrs(op.child);
	}
	
	/*
	 * This function is called recursively to initialize index scan operators.
	 * @param
	 * 		op: the operator that is trying to initialize index scan.
	 */
	private void initializeIndexScans(PhyOp op) {
		if (op == null)
			return;
		if (op instanceof PhyJoinOp) {
			initializeIndexScans(op.child);
			initializeIndexScans(((PhyJoinOp) op).rChild);
		} else if (op instanceof PhySortOp) {
			initializeIndexScans(op.child);
		} else if (op instanceof PhyScanBfOp) {
			return;
		} else if (op instanceof PhyScanIndexOp) {
			((PhyScanIndexOp) op).initialize(null);
		}
		return;
	}
	
	/*
	 * This function builds physical plan according to optimized plan.
	 */
	private void buildOptimized() {
		PhyOp dataRoot;
		
		PhyPlanOptimizer optimize = new PhyPlanOptimizer(logPlan);
		
		// Only one scan, no join.
		if (optimize.finalJoinOrder == null) {
			LogPlan.Scan scan = logPlan.joinChildren.get(0);
			PhyPlanOptimizer.ScanInfo scanInfo = optimize.finalScanPlan.get(scan.alias);
			dataRoot = buildOptimizedScan(scan, scanInfo);
		} else {	// Joins
			
			// Left most scan.
			LogPlan.Scan scan = logPlan.joinChildren.get(optimize.finalJoinOrder.get(0));
			PhyPlanOptimizer.ScanInfo scanInfo = optimize.finalScanPlan.get(scan.alias);
			PhyCondOp leftTree = buildOptimizedScan(scan, scanInfo);
			PhyScanOp rightTree;
			
			for (int i = 0; i < optimize.finalJoinType.size(); ++i) {
				scan = logPlan.joinChildren.get(optimize.finalJoinOrder.get(i + 1));
				scanInfo = optimize.finalScanPlan.get(scan.alias);
				rightTree = buildOptimizedScan(scan, scanInfo);
				
				PhyJoinOp join;
				switch (optimize.finalJoinType.get(i)) {
				case 1:			// BNLJ
					String append = (DBCatalog.getCatalog().inputPath.contains("/") ? "db/data/" : "db\\data\\");
					join = new PhyJoinBNLJOp(joinBuffer, 	DBCatalog.getCatalog().tables.get(rightTree.fileName).attrs.size(),
															DBCatalog.getCatalog().inputPath + append + rightTree.fileName);
					join.conditions = optimize.finalJoinCond.get(i);
					join.child = leftTree;
					join.rChild = rightTree;
					leftTree = join;
					break;
				case 2:			// SMJ
					join = new PhyJoinSMJOp();
					PhySortOp lSort = new PhySortExOp(sortBuffer);
					PhySortOp rSort = new PhySortExOp(sortBuffer);
					
					for (Condition cond : optimize.finalJoinCond.get(i)) {		// Optimizer has made left and right accordingly.
						if (cond.operator == Condition.op.e) {					// Equal conditions.
							join.conditions.add(cond);
							if (!lSort.sortAttrs.contains(cond.leftName))		// No repetition.
								lSort.sortAttrs.add(cond.leftName);
							if (!rSort.sortAttrs.contains(cond.rightName))
								rSort.sortAttrs.add(cond.rightName);
						} else {
							((PhyJoinSMJOp)join).extraConditions.add(cond);		// Non-equal conditions.
						}
					}
					
					lSort.child = leftTree;
					rSort.child = rightTree;
					join.child = lSort;
					join.rChild = rSort;
					leftTree = join;
					break;
				default:
					break;
				}
			}
			dataRoot = leftTree;
		}
		
		// If select everything, projection operator will be responsible for order.
		PhyProjOp projOp = new PhyProjBfOp();
		if (logPlan.projAttrs == null) {
			projOp.selectAll = true;
		} else {
			projOp.selectAll = false;
			projOp.projAttrs = logPlan.projAttrs;
		}
		projOp.child = dataRoot;
		projOp.buildSchema();
		root = projOp;
		
		// If select everything, modify schema so that projection will change output order.
		if (projOp.selectAll) {
			int count = 0;
			for (LogPlan.Scan scan : logPlan.joinChildren) {		// joinChildren follows output order.
				DBCatalog.RelationInfo relationInfo = DBCatalog.getCatalog().tables.get(scan.fileName);
				for (DBCatalog.AttrInfo attrInfo : relationInfo.attrs)
					projOp.schema.replace(scan.alias + '.' + attrInfo.name, count++);
			}
		}
		
		// Order operator
		if (logPlan.orderAttrs != null || logPlan.hasDist) {
			PhySortOp orderOp = new PhySortExOp(sortBuffer);
			if (logPlan.orderAttrs != null)
				orderOp.sortAttrs = logPlan.orderAttrs;
			orderOp.child = root;
			orderOp.schema = root.schema;
			root = orderOp;
		}
		
		// Distinct operator
		if (logPlan.hasDist) {
			PhyDistOp distOp = new PhyDistBfOp();
			distOp.hasOrderby = true;
			distOp.child = root;
			distOp.schema = root.schema;
			root = distOp;
		}
	}
	
	/*
	 * This function builds scan operator according to optimization information.
	 * @param
	 * 		scan: information about this scan in logical plan.
	 * 		scanInfo: information about this scan in optimization.
	 */
	private PhyScanOp buildOptimizedScan(LogPlan.Scan scan, PhyPlanOptimizer.ScanInfo scanInfo) {
		if (scanInfo.type == 0) {							// Full scan.
			PhyScanBfOp scanOp = new PhyScanBfOp();
			scanOp.alias = scan.alias;
			scanOp.fileName = scan.fileName;
			for (Entry<String, LogPlan.HighLowCondition> entry : scan.conditions.entrySet()) {
				if (entry.getValue().highValue != Integer.MAX_VALUE)
					scanOp.conditions.add(new Condition(entry.getKey() + " <= " + entry.getValue().highValue));
				if (entry.getValue().lowValue != Integer.MIN_VALUE)
					scanOp.conditions.add(new Condition(entry.getKey() + " >= " + entry.getValue().lowValue));
			}
			scanOp.conditions.addAll(scan.otherConditions);
			return scanOp;
		} else {											// Index scan.
			PhyScanIndexOp scanOp = new PhyScanIndexOp(scan.fileName, scan.alias, scanInfo.keyName);
			scanOp.initialize(scan);
			return scanOp;
		}
	}
}
