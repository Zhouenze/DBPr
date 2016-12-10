package physicalPlan;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map.Entry;

import base.Condition;
import base.DBCatalog;
import logicalPlan.LogPlan;

/**
 * Physical Plan class
 * This class is also a visitor to logical plan, so as to build a physical plan.
 * 
 * @author Enze Zhou ez242
 */
public final class PhyPlan implements PhyOpVisitor {

	String query = "";							// Original query.
	private LogPlan logPlan = null;				// Logical plan that is being translated.
	private PhyPlanOptimizer optimize = null;	// Optimizer of this query
	public PhyOp root = null;					// Root node of this physical plan.
	
	private final int joinBuffer = 10;			// Config buffer size of BNLJ, set to constant as instructed.
	private final int sortBuffer = 10;			// Config buffer size of external sort, set to constant as instructed.
	
	
	/**
	 * Another constructor of this class, ready for future usage.
	 * @param logPlan
	 * 		Logical plan that is being translated into a physical plan.
	 * @param config
	 * 		Configuration string that controls physical plan building.
	 */
	public PhyPlan(LogPlan logPlan) {
		query = logPlan.query;
		this.logPlan = logPlan;
		buildOptimized();
	}
	
	
	/*
	 * This function builds physical plan according to optimized plan.
	 */
	private void buildOptimized() {
		PhyOp dataRoot;
		
		optimize = new PhyPlanOptimizer(logPlan);
		
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
			projOp.schema = new HashMap<>();
			int count = 0;
			for (LogPlan.Scan scan : logPlan.joinChildren) {		// joinChildren follows output order.
				DBCatalog.RelationInfo relationInfo = DBCatalog.getCatalog().tables.get(scan.fileName);
				for (DBCatalog.AttrInfo attrInfo : relationInfo.attrs)
					projOp.schema.put(scan.alias + '.' + attrInfo.name, count++);
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
	
	/**
	 * This function builds scan operator according to optimization information.
	 * @param
	 * 		scan: information about this scan in logical plan.
	 * 		scanInfo: information about this scan in optimization.
	 * @return
	 * 		configured scan operator
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
	
	// These elements are used to print this physical plan.
	String printString = null;	// If not null, this is the output of this plan.
	byte [] layers = null;		// Used to generate "---" faster.
	int layer = 0;				// Used to note which layer it is.
	
	/**
	 * This function returns string representation of this physical plan.
	 * @return
	 * 		String representation of this plan.
	 */
	public String toString() {
		if (printString == null) {			// If string representation not built before, build it.
			printString = "";
			layer = 0;
			layers = new byte [20];
			for (int i = 0; i < 20; ++i)
				layers[i] = '-';
			root.accept(this);				// String representation is built with visitor pattern.
		}
		return printString;
	}
	
	/**
	 * print this plan to some output stream
	 * @param
	 * 		out: the output stream to be printed to.
	 */
	public void print(OutputStream out) {
		
		// By default, print to standard output.
		if (out == null)
			out = System.out;
		
		try {
			out.write(this.toString().getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Use visitor pattern to build string representation. Mainly distribute.
	 * @override from PhyOpVisitor interface
	 * @param phyOp
	 * 		Physical operator that is being visited.
	 */
	@Override
	public void visit(PhyOp phyOp) {
		if (phyOp instanceof PhyDistOp)
			visit((PhyDistOp) phyOp);
		else if (phyOp instanceof PhySortOp)
			visit((PhySortOp) phyOp);
		else if (phyOp instanceof PhyProjOp)
			visit((PhyProjOp) phyOp);
		else if (phyOp instanceof PhyJoinOp)
			visit((PhyJoinOp) phyOp);
		else if (phyOp instanceof PhyScanOp)
			visit((PhyScanOp) phyOp);
	}

	/**
	 * Use visitor pattern to build string representation.
	 * @override from PhyOpVisitor interface
	 * @param phyDistOp
	 * 		Distinct operator that is being visited.
	 */
	@Override
	public void visit(PhyDistOp PhyDistOp) {
		printString += "DupElim\n";
		++layer;
		PhyDistOp.child.accept(this);
		--layer;
	}

	/**
	 * Use visitor pattern to build string representation.
	 * @override from PhyOpVisitor interface
	 * @param phyJoinOp
	 * 		Join operator that is being visited.
	 */
	@Override
	public void visit(PhyJoinOp PhyJoinOp) {
		printString += new String(layers, 0, layer);
		printString += (PhyJoinOp.toString() + '\n');
		++layer;
		PhyJoinOp.child.accept(this);
		PhyJoinOp.rChild.accept(this);
		--layer;
	}

	/**
	 * Use visitor pattern to build string representation.
	 * @override from PhyOpVisitor interface
	 * @param phyProjOp
	 * 		Projection operator that is being visited.
	 */
	@Override
	public void visit(PhyProjOp PhyProjOp) {
		if (logPlan.projAttrs != null) {
			printString += new String(layers, 0, layer);
			printString += String.format("Project%s\n", PhyProjOp.projAttrs.toString());
			++layer;
			PhyProjOp.child.accept(this);
			--layer;
		} else {
			PhyProjOp.child.accept(this);
		}
	}

	/**
	 * Use visitor pattern to build string representation.
	 * @override from PhyOpVisitor interface
	 * @param phyScanOp
	 * 		Scan operator that is being visited.
	 */
	@Override
	public void visit(PhyScanOp PhyScanOp) {
		printString += new String(layers, 0, layer);
		if (!PhyScanOp.conditions.isEmpty()) {
			printString += String.format("Select%s\n", PhyScanOp.conditions.toString());
			printString += new String(layers, 0, layer + 1);
			printString += (PhyScanOp.toString() +'\n');
		} else {
			printString += (PhyScanOp.toString() + '\n');
		}
	}

	/**
	 * Use visitor pattern to build string representation.
	 * @override from PhyOpVisitor interface
	 * @param phySortOp
	 * 		Sort operator that is being visited.
	 */
	@Override
	public void visit(PhySortOp PhySortOp) {
		printString += new String(layers, 0, layer);
		printString += String.format("ExternalSort%s\n", PhySortOp.sortAttrs.toString());
		++layer;
		PhySortOp.child.accept(this);
		--layer;
	}
}
