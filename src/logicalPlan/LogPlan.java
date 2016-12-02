package logicalPlan;
import java.util.HashMap;
import java.util.Vector;

import base.Condition;
import base.Condition.op;

import java.util.Iterator;
import java.util.Map;

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
	
	
	
	
	
	public Vector<Condition> joinConditions = new Vector<>();	// All the conditions on join operator. Full name. Should include pushed conditions.
	
	public class HighLowCondition {
		public Integer lowValue = Integer.MIN_VALUE;			// attr >= lowValue. If not exist, set to Integer.MIN_VALUE
		public Integer highValue = Integer.MAX_VALUE;			// attr <= highValue. If not exist, set to Integer.MAX_VALUE
	}
	
	public class Scan {
		public String fileName;
		public String alias;
		public HashMap<String, HighLowCondition> conditions = new HashMap<>();	// This part map an attribute to it's high-low value. Full name.
																				// Every attribute that has some restriction including >, >=, <, <=, = constant should be here.
																				// Otherwise should not be one for it.
		public Vector<Condition> otherConditions = new Vector<>();				// This part is other conditions, including R.A < R.B, R.A <> 0, etc
	}
	
	public Map<String, Scan> aliasMap = new HashMap<>();        // The map to 
	public Vector<Scan> joinChildren = new Vector<>();			// Children of join represented by Scan, in output order 
																// (from A, B => joinChildren[0].fileName == A && joinChildren[1].fileName == B).
																// If only one relation, it should also appear here.
	public Map<String, String> aliasDict = new HashMap<>();	    // A dictionary to change alias to filename. If no alias is used, alias is the same as filename.
	public Vector<String> orderAttrs = null;					// Order by attributes, in full name. null means there is no order by operator.
	public Vector<String> projAttrs = null;						// Projection attributes, in full name. null means select all.
	public boolean hasDist = false;								// Whether there is a distinct operator.
	
	
	// About union-found: every attribute should be in some union, may be single-element union.
	// Union Find
	public Map<String, Cluster> union_find = new HashMap<String, Cluster>();

	private class Cluster{
		// 貌似是unnecessary的
		// but I need a way to link cluster back to the attrs it has
		public Vector<String> attrs = new Vector<String>(); // 指向这个cluster就表示是这个cluster的attr啦？///////////////////
		public HighLowCondition condts = null;
		
	}
	
	// 直接更新这个key(attr)指向的value(cluster)就相当于把其他指向这个cluster的key的value也改了..吧？
	// 所以find不用做啥了？ union做好就行
	private Cluster find(String attr) {
		if(!union_find.containsKey(attr)) {
			Cluster cluster = new Cluster();
			union_find.put(attr, cluster);
			cluster.attrs.add(attr);
		}
		return union_find.get(attr);
	}
	
	private void union(Cluster c1, Cluster c2) { // 更新到c1里面
		// c2 attr -> c1 cluster
		for(String attr: c2.attrs) {
			union_find.put(attr, c1);
		}
		// c2 attr in c1.attrs
		for(String attr: c2.attrs) {
			c1.attrs.add(attr);
		}
		// update c1 HighLow
		// assume no "bad" where clauses 没有交集的情况不考虑 //////////////////////////////
		if(c1.condts == null) {
			c1.condts = new HighLowCondition();
		}
		if(c2.condts == null) {
			c2.condts = new HighLowCondition();
		}
		c1.condts.lowValue = c1.condts.lowValue > c2.condts.lowValue ? c1.condts.lowValue : c2.condts.lowValue;
		c1.condts.highValue = c1.condts.highValue < c2.condts.lowValue ? c1.condts.highValue : c2.condts.highValue;
		
	}
	
	/**
	 * Method to set or update numeric bound in the given union-find element, i.e. the given Cluster
	 * @param c is the cluster whose numeric bound needs to be set or update.
	 * @param crrt is the condition cluster c needs to be updated upon.
	 */
	private void setConds(Cluster c, Condition crrt) {
		
		if (c.condts == null) {
			c.condts = new HighLowCondition();
		} 
		
		if (crrt.operator == op.g || crrt.operator == op.ge) {
			c.condts.lowValue = c.condts.lowValue > crrt.right ? c.condts.lowValue : crrt.right;
		} else if (crrt.operator == op.l || crrt.operator == op.le) {
			c.condts.highValue = c.condts.highValue < crrt.right ? c.condts.highValue : crrt.right;
		} else if (crrt.operator == op.e) { // 可能会有问题 还没想明白 //////////////
			c.condts.lowValue = crrt.right;
			c.condts.highValue = crrt.right;
		}
		
	}
	
	
	
	
	private LogSortOp sort = null;						// Sort logical operator in this plan.
	private LogProjOp proj = null;						// Projection logical operator in this plan.
	
	public String query = "";									// Original query.
	public Vector<Condition> conditions = new Vector<>();		// All the conditions in this query. Condition distribution will be postponed to physical plan 
																// building for convenience of future optimization.
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
		
		// From Clause
		// Build Scan objects; Prepare them for join if more than 1
		// naiveJoinOrder这个instance variable还要吗？////////////////////////////////////
		if (plainSelect.getJoins() == null) {
			
			// # of scan = 1
			Scan tempScan = new Scan();
			tempScan.fileName = plainSelect.getFromItem().toString().split(" ")[0];
			tempScan.alias = plainSelect.getFromItem().getAlias() == null ? tempScan.fileName : plainSelect.getFromItem().getAlias();
			aliasMap.put(tempScan.alias, tempScan);
			this.aliasDict.put(tempScan.alias, tempScan.fileName);
			this.naiveJoinOrder.add(tempScan.alias);
			this.joinChildren.add(tempScan);
			
			// proj怎么和scan连起来？要改LogProjOp吗？不然怎么弄的 //////////////
			
		} else {
			
			// # of scan > 1
			Scan tempScan = new Scan();
			tempScan.fileName = plainSelect.getFromItem().toString().split(" ")[0];
			tempScan.alias = plainSelect.getFromItem().getAlias() == null ? tempScan.fileName : plainSelect.getFromItem().getAlias();
			aliasMap.put(tempScan.alias, tempScan);
			this.aliasDict.put(tempScan.alias, tempScan.fileName);
			this.naiveJoinOrder.add(tempScan.alias);
			this.joinChildren.add(tempScan);
			
			Iterator joinsIt = plainSelect.getJoins().iterator();
			Join join = (Join) joinsIt.next();
			tempScan = new Scan();
			tempScan.fileName = join.getRightItem().toString().split(" ")[0];
			tempScan.alias = (join.getRightItem().getAlias() == null ? tempScan.fileName : join.getRightItem().getAlias());
			aliasMap.put(tempScan.alias, tempScan);
			aliasDict.put(tempScan.alias, tempScan.fileName);
			naiveJoinOrder.add(tempScan.alias);
			
			// Add the rest "join"
			while (joinsIt.hasNext()) {
				join = (Join) joinsIt.next();
				tempScan = new Scan();
				tempScan.fileName = join.getRightItem().toString().split(" ")[0];
				tempScan.alias = (join.getRightItem().getAlias() == null ? tempScan.fileName : join.getRightItem().getAlias());
				aliasMap.put(tempScan.alias, tempScan);
				aliasDict.put(tempScan.alias, tempScan.fileName);
				naiveJoinOrder.add(tempScan.alias);
			}
		}
		
		// Where Clause
		// Build up Union Find, then update Scan objects
		// Note that we won't have such condition as val OP val this time.
		// conditions这个instance variable还要吗？/////////////////////////////
		if (plainSelect.getWhere() != null) {
			String [] whereClauses;
			whereClauses = plainSelect.getWhere().toString().split("AND");
			for (int i = 0; i < whereClauses.length; ++i) {
				Condition crrt = new Condition(whereClauses[i]);
				conditions.add(crrt);
				
				if (crrt.isEquality()) { // isEquality: union
					Cluster c1 = this.find(crrt.leftName);
					Cluster c2 = this.find(crrt.rightName);
					this.union(c1, c2);
				} else if (crrt.isUsable()) { // isUsable: update numeric bound
					Cluster c = this.find(crrt.leftName);
					this.setConds(c, crrt);
				} else { // other conditions: directly updated in Scan objects
					// attr OP attr 这种是要把两个attr对应的table的Scan obj都更新是吧？是..///////
					// attr name的格式是R.A这种吧 /////////
					String alias = crrt.leftName.split(".")[0];
					aliasMap.get(alias).otherConditions.add(crrt);
					
					if(crrt.rightName != null) {
						alias = crrt.rightName.split(".")[0];
						aliasMap.get(alias).otherConditions.add(crrt);
					}
				}
				
			}
			
			// update Scan objects according to union_find, i.e. the result of union find procedure
			// from attr names, separate out table alias, link to Scan obj., update its conditions map
			for(String attr: union_find.keySet()) {
				String alias = attr.split(".")[0];
				aliasMap.get(alias).conditions.put(attr, union_find.get(attr).condts);
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
