package logicalPlan;
import java.util.HashMap;
import java.util.Vector;

import base.Condition;

import java.util.Iterator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

/*
 * Logical Plan class
 * This class is also a visitor to sql queries, so as to build a logical plan.
 * 
 * @author Enze Zhou ez242
 */
public final class LogPlan implements SelectVisitor, FromItemVisitor {
	
	
	
	
	
	public Vector<Condition> joinConditions = new Vector<>();
	public class pushedConditions {
		String attrName;
		Integer lowValue;
		Integer highValue;
	}
	public class scan {
		String fileName;
		String alias;
		Vector<pushedConditions> conditions = new Vector<>();
	}
	public Vector<scan> joinChildren = new Vector<>();
	public Vector<String> outputOrder = new Vector<>();
	
	
	
	
	
	
	
	private LogSortOp sort = null;						// Sort logical operator in this plan.
	private LogProjOp proj = null;						// Projection logical operator in this plan.
	
	public String query = "";									// Original query.
	public Vector<Condition> conditions = new Vector<>();		// All the conditions in this query. Condition distribution will be postponed to physical plan 
																// building for convenience of future optimization.
	public HashMap<String, String> aliasDict = new HashMap<>();	// A dictionary to change alias to filename. If no alias is used, alias is the same as filename.
	public Vector<String> naiveJoinOrder = new Vector<>();		// For a naive plan, the order of scan is the same as input. This field will be deleted after 
																// optimization project is done because then we will decide the order on our own.
	
	public LogOp root = null;							// Root node of this logical plan.
	public LogOp dataRoot = null;						// Root node of the data part of this plan, maybe a join operator or a scan operator.
	
	/*
	 * Constructor of this class.
	 * Copy original query and use visitor pattern to build plan.
	 * @param select
	 * 		The select query that is being built into a logical plan.
	 */
	public LogPlan(Select select) {
		query = select.toString();
		select.getSelectBody().accept(this);
	}

	/*
	 * Visit a plainselect clause and build logical plan.
	 * @param plainSelect
	 * 		Clause that is being built.
	 */
	@Override
	public void visit(PlainSelect plainSelect) {
		
		LogOp temp = null;
		
		// If there is a distinct, build it.
		if (plainSelect.getDistinct() != null) {
			root = temp = new LogDistOp();
		}
		
		// If there is a sort, build it.
		if (plainSelect.getOrderByElements() != null) {
			if (temp != null) {
				((LogDistOp)temp).hasOrderby = true;
				temp.child = sort = new LogSortOp();
				temp = temp.child;
			} else {
				root = temp = sort = new LogSortOp();
			}
			
			// Get all the sort attributes and save them to sort operator.
			Iterator orderIterator = plainSelect.getOrderByElements().iterator();
			while (orderIterator.hasNext()) {
				String str = orderIterator.next().toString();
				sort.sortAttrs.add(str);
			}
		} else if (temp != null) {
			((LogDistOp)temp).hasOrderby = true;
			temp.child = sort = new LogSortOp();
			temp = temp.child;
		}
		
		// Make a projection operator which is mandatory.
		if (temp != null) {
			temp.child = proj = new LogProjOp();
			temp = temp.child;
		} else {
			root = temp = proj = new LogProjOp();
		}
		
		// Get all the projection target. If *, set selectAll. If no *, insert all the others to projection operator.
		proj.selectAll = plainSelect.getSelectItems().get(0).toString().equals("*");
		if (!proj.selectAll) {
			Iterator projIterator = plainSelect.getSelectItems().iterator();
			while (projIterator.hasNext()) {
				String str = projIterator.next().toString();
				proj.projAttrs.add(str);
			}
		}
		
		// Build scan operators and join operators.
		if (plainSelect.getJoins() == null) {
			
			// If there is only one scan.
			LogScanOp tempScan = new LogScanOp();
			String fileName = plainSelect.getFromItem().toString().split(" ")[0];
			String alias = (plainSelect.getFromItem().getAlias() == null ? fileName : plainSelect.getFromItem().getAlias());
			aliasDict.put(alias, fileName);
			naiveJoinOrder.add(alias);
			proj.child = dataRoot = tempScan;
			
		} else {
			
			// If there are many tables to join.
			// Build the leftmost two scan nodes and join node.
			LogJoinOp joinRoot = new LogJoinOp();
			LogJoinOp tempJoin;
			LogScanOp tempScan = new LogScanOp();
			String fileName = plainSelect.getFromItem().toString().split(" ")[0];
			String alias = (plainSelect.getFromItem().getAlias() == null ? fileName : plainSelect.getFromItem().getAlias());
			aliasDict.put(alias, fileName);
			naiveJoinOrder.add(alias);
			joinRoot.child = tempScan;
			
			Iterator joinsIt = plainSelect.getJoins().iterator();
			Join join = (Join) joinsIt.next();
			tempScan = new LogScanOp();
			fileName = join.getRightItem().toString().split(" ")[0];
			alias = (join.getRightItem().getAlias() == null ? fileName : join.getRightItem().getAlias());
			aliasDict.put(alias, fileName);
			naiveJoinOrder.add(alias);
			joinRoot.rChild = tempScan;
			
			// Then build all the others into a left-deep join tree.
			while (joinsIt.hasNext()) {
				join = (Join) joinsIt.next();
				tempJoin = new LogJoinOp();
				tempJoin.child = joinRoot;
				tempScan = new LogScanOp();
				fileName = join.getRightItem().toString().split(" ")[0];
				alias = (join.getRightItem().getAlias() == null ? fileName : join.getRightItem().getAlias());
				aliasDict.put(alias, fileName);
				naiveJoinOrder.add(alias);
				tempJoin.rChild = tempScan;
				joinRoot = tempJoin;
			}
			proj.child = dataRoot = joinRoot;
		}
		
		// If there are where clauses
		if (plainSelect.getWhere() != null) {
			String [] whereClauses;
			whereClauses = plainSelect.getWhere().toString().split("AND");
			for (int i = 0; i < whereClauses.length; ++i) {
				conditions.add(new Condition(whereClauses[i]));
			}
		}
	}

	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(Union Union) {}
	
	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(Table Table) {}
	
	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(SubSelect SubSelect) {}
	
	/*
	 * Required by interface but no implementation provided because it's unnecessary.
	 */
	@Override
	public void visit(SubJoin SubJoin) {}
	
}
