package logicalPlan;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import base.Condition;
import base.Condition.op;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

/**
 * Logical Plan class
 * This class is also a visitor to sql queries, so as to build a logical plan.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468
 */
public final class LogPlan implements SelectVisitor, FromItemVisitor {
	
	public Vector<Condition> joinConditions = new Vector<>();	// All the conditions on join operator. Full name. Should include pushed conditions.
	
	/*
	 * This class represent a condition with upper bound and lower bound
	 */
	public class HighLowCondition {
		public Integer lowValue = Integer.MIN_VALUE;			// attr >= lowValue. If not exist, set to Integer.MIN_VALUE
		public Integer highValue = Integer.MAX_VALUE;			// attr <= highValue. If not exist, set to Integer.MAX_VALUE
	}
	
	/*
	 * This class represents all the information of a scan
	 */
	public class Scan {
		public String fileName;
		public String alias;
		public HashMap<String, HighLowCondition> conditions = new HashMap<>();	// This part map an attribute to it's high-low value. Full name.
																				// Every attribute that has some restriction including >, >=, <, <=, = constant should be here.
																				// Otherwise should not be one for it.
		public Vector<Condition> otherConditions = new Vector<>();				// This part is other conditions, including R.A < R.B, R.A <> 0, etc
	}
	
	public Map<String, Scan> aliasMap = new HashMap<>();        // The map from table alias to its Scan object
	public Vector<Scan> joinChildren = new Vector<>();			// Children of join represented by Scan, in output order 
																// (from A, B => joinChildren[0].fileName == A && joinChildren[1].fileName == B).
																// If only one relation, it should also appear here.
	public Vector<String> orderAttrs = null;					// Order by attributes, in full name. null means there is no order by operator.
	public Vector<String> projAttrs = null;						// Projection attributes, in full name. null means select all.
	public boolean hasDist = false;								// Whether there is a distinct operator.
	
	public String query = "";									// Original query.

	
	// Union Find is represented as a map from attributes (full name) to clusters
	public Map<String, Cluster> union_find = new HashMap<String, Cluster>();

	/*
	 * This class represents a union.
	 */
	public class Cluster{
		// 璨屼技鏄痷nnecessary鐨�
		// but I need a way to link cluster back to the attrs it has
		
		// attributes in this union
		public Vector<String> attrs = new Vector<String>(); // 鎸囧悜杩欎釜cluster灏辫〃绀烘槸杩欎釜cluster鐨刟ttr鍟︼紵///////////////////
		public HighLowCondition condts = null;				// bound of all the attributes in this union
		
	}
	
	// 鐩存帴鏇存柊杩欎釜key(attr)鎸囧悜鐨剉alue(cluster)灏辩浉褰撲簬鎶婂叾浠栨寚鍚戣繖涓猚luster鐨刱ey鐨剉alue涔熸敼浜�..鍚э紵
	// 鎵�浠ind涓嶇敤鍋氬暐浜嗭紵 union鍋氬ソ灏辫
	/**
	 * This function find the union which an attribute belongs to.
	 * @param
	 * 		attr: the attribute that is being found
	 * @return
	 * 		the union that contains the attribute
	 */
	public Cluster find(String attr) {
		if(!union_find.containsKey(attr)) {
			Cluster cluster = new Cluster();
			union_find.put(attr, cluster);
			cluster.attrs.add(attr);
		}
		return union_find.get(attr);
	}
	
	/**
	 * This function combines two unions
	 * @param
	 * 		c1, c2: the two clusters being unioned
	 */
	private void union(Cluster c1, Cluster c2) { // 鏇存柊鍒癱1閲岄潰
		
		// Avoid dead loop
		if (c1 == c2)
			return;
		
		// c2 attr -> c1 cluster
		for(String attr: c2.attrs) {
			union_find.remove(attr);
			union_find.put(attr, c1);
		}
		// c2 attr in c1.attrs
		for(String attr: c2.attrs) {
			c1.attrs.add(attr);
		}
		// update c1 HighLow
		if(c1.condts == null) {
			c1.condts = new HighLowCondition();
		}
		if(c2.condts == null) {
			c2.condts = new HighLowCondition();
		}
		c1.condts.lowValue = c1.condts.lowValue > c2.condts.lowValue ? c1.condts.lowValue : c2.condts.lowValue;
		c1.condts.highValue = c1.condts.highValue < c2.condts.highValue ? c1.condts.highValue : c2.condts.highValue;
		
		union_find.values().remove(c2);
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
			int right = crrt.operator == op.g ? crrt.right + 1 : crrt.right;
			c.condts.lowValue = c.condts.lowValue > right ? c.condts.lowValue : right;
		} else if (crrt.operator == op.l || crrt.operator == op.le) {
			int right = crrt.operator == op.l ? crrt.right - 1 : crrt.right;
			c.condts.highValue = c.condts.highValue < right ? c.condts.highValue : right;
		} else if (crrt.operator == op.e) { // 鍙兘浼氭湁闂 杩樻病鎯虫槑鐧� //////////////
			c.condts.lowValue = crrt.right;
			c.condts.highValue = crrt.right;
		}
		
	}	
	
	/**
	 * Constructor of this class.
	 * Copy original query and use visitor pattern to build plan.
	 * @param select
	 * 		The select query that is being built into a logical plan.
	 */
	public LogPlan(Select select) {
		query = select.toString();
		select.getSelectBody().accept(this);
	}

	/**
	 * Visit a plainselect clause and build logical plan.
	 * @param plainSelect
	 * 		Clause that is being built.
	 */
	@Override
	public void visit(PlainSelect plainSelect) {
		
		// If there is a distinct, build it.
		if (plainSelect.getDistinct() != null) {
			this.hasDist = true;
		}
		
		// If there is a sort, build it.
		if (plainSelect.getOrderByElements() != null) {
			this.orderAttrs = new Vector<>();
			
			// Get all the sort attributes and save them to sort operator.
			Iterator orderIterator = plainSelect.getOrderByElements().iterator();
			while (orderIterator.hasNext()) {
				String str = orderIterator.next().toString();
				this.orderAttrs.add(str);
			}
		} 
		
		// Get all the projection target. If *, set selectAll. If no *, insert all the others to projection operator.
		boolean ifSelectAll = plainSelect.getSelectItems().get(0).toString().equals("*");
		if (!ifSelectAll) {
			this.projAttrs = new Vector<>();
			
			Iterator projIterator = plainSelect.getSelectItems().iterator();
			while (projIterator.hasNext()) {
				String str = projIterator.next().toString();
				this.projAttrs.add(str);
			}
		}
		
		// From Clause
		// Build Scan objects; Prepare them for join if more than 1
		if (plainSelect.getJoins() == null) {
			
			// # of scan = 1
			Scan tempScan = new Scan();
			tempScan.fileName = plainSelect.getFromItem().toString().split(" ")[0];
			tempScan.alias = plainSelect.getFromItem().getAlias() == null ? tempScan.fileName : plainSelect.getFromItem().getAlias();
			aliasMap.put(tempScan.alias, tempScan);
			this.joinChildren.add(tempScan);
			
			
		} else {
			
			// # of scan > 1
			Scan tempScan = new Scan();
			tempScan.fileName = plainSelect.getFromItem().toString().split(" ")[0];
			tempScan.alias = plainSelect.getFromItem().getAlias() == null ? tempScan.fileName : plainSelect.getFromItem().getAlias();
			aliasMap.put(tempScan.alias, tempScan);
			this.joinChildren.add(tempScan);
			
			Iterator joinsIt = plainSelect.getJoins().iterator();
			Join join = (Join) joinsIt.next();
			tempScan = new Scan();
			tempScan.fileName = join.getRightItem().toString().split(" ")[0];
			tempScan.alias = (join.getRightItem().getAlias() == null ? tempScan.fileName : join.getRightItem().getAlias());
			aliasMap.put(tempScan.alias, tempScan);
			this.joinChildren.add(tempScan);
			
			// Add the rest "join"
			while (joinsIt.hasNext()) {
				join = (Join) joinsIt.next();
				tempScan = new Scan();
				tempScan.fileName = join.getRightItem().toString().split(" ")[0];
				tempScan.alias = (join.getRightItem().getAlias() == null ? tempScan.fileName : join.getRightItem().getAlias());
				aliasMap.put(tempScan.alias, tempScan);
				this.joinChildren.add(tempScan);
			}
		}
		
		// Where Clause
		// Build up Union Find, then update Scan objects
		// Note that we won't have such condition as val OP val this time.
		if (plainSelect.getWhere() != null) {
			String [] whereClauses;
			whereClauses = plainSelect.getWhere().toString().split("AND");
			for (int i = 0; i < whereClauses.length; ++i) {
				Condition crrt = new Condition(whereClauses[i]);
				
				if (crrt.isEquality()) { // isEquality: union
					Cluster c1 = this.find(crrt.leftName);
					Cluster c2 = this.find(crrt.rightName);
					this.union(c1, c2);
				} else if (crrt.isUsable()) { // isUsable: update numeric bound
					Cluster c = this.find(crrt.leftName);
					this.setConds(c, crrt);
				} else { // other conditions
					// attr OP attr 杩欑鏄鎶婁袱涓猘ttr瀵瑰簲鐨則able鐨凷can obj閮芥洿鏂版槸鍚э紵鏄�..///////
					String alias = crrt.leftName.split("\\.")[0];
					if (crrt.rightName != null && !crrt.rightName.split("\\.")[0].equals(alias)) {
						joinConditions.add(crrt);
					} else {
						aliasMap.get(alias).otherConditions.add(crrt);
					}
				}
			}
			
			// update Scan objects according to union_find, i.e. the result of union find procedure
			// from attr names, separate out table alias, link to Scan obj., update its conditions map
			for(String attr: union_find.keySet()) {
				String alias = attr.split("\\.")[0];
				aliasMap.get(alias).conditions.put(attr, union_find.get(attr).condts);
			}
			
			// Add equality conditions implied by unions.
			HashSet<String> seenAttr = new HashSet<>();
			for (Cluster clu : union_find.values()) {
				if (seenAttr.contains(clu.attrs.get(0)))
					continue;
				else
					seenAttr.add(clu.attrs.get(0));
				
				for (int i = 0; i < clu.attrs.size(); ++i) {
					String attr = clu.attrs.get(i);
					String ali = attr.split("\\.")[0];
					for (int j = i + 1; j < clu.attrs.size(); ++j) {
						String attr2 = clu.attrs.get(j);
						if (ali.equals(attr2.split("\\.")[0])) {
							aliasMap.get(ali).otherConditions.add(new Condition(attr + " = " + attr2));
						} else {
							joinConditions.add(new Condition(attr + " = " + attr2));
						}
					}
				}
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
	
	/**
	 * This function prints the information of this logical plan to some output stream
	 * @param out
	 * 		print destination stream.
	 */
	public void print(OutputStream out) {
		
		// default: output to standard output
		if (out == null)
			out = System.out;
		
		try {
			// For better performance, use this to build "---" strings.
			byte [] layers = new byte [20];
			for (int i = 0; i < layers.length; ++i)
				layers[i] = '-';
			
			int layer = 0;
			
			// print distinct
			if (hasDist) {
				out.write("DupElim\n".getBytes());
				++layer;
			}
			
			// print order. order operator introduced by distinct need to be here.
			if (orderAttrs != null || hasDist) {
				out.write(layers, 0, layer);
				if (orderAttrs != null)
					out.write(String.format("Sort%s\n", orderAttrs.toString()).getBytes());
				else
					out.write("Sort[]\n".getBytes());
				++layer;
			}
			
			// print projection. If select all, no projection print.
			if (projAttrs != null) {
				out.write(layers, 0, layer);
				out.write(String.format("Project%s\n", projAttrs.toString()).getBytes());
				++layer;
			}
			
			// print join
			if (joinChildren.size() > 1) {
				
				// print conditions of join
				out.write(layers, 0, layer);
				ArrayList<String> conditionsStrings = new ArrayList<>();
				for (Condition cond : joinConditions)
					if (cond.operator != op.e)			// conditions introduced by union-find are not considered residual conditions.
						conditionsStrings.add(cond.toString());
				if (conditionsStrings.isEmpty())
					out.write("Join[null]\n".getBytes());
				else
					out.write(String.format("Join[%s]\n", String.join(" AND ", conditionsStrings)).getBytes());
				
				// print union find
				HashSet<String> seenAttrs = new HashSet<>();
				for (Cluster clu : union_find.values()) {
					if (seenAttrs.contains(clu.attrs.get(0)))
						continue;
					else
						seenAttrs.add(clu.attrs.get(0));
					
					out.write(String.format("[%s, equals ", clu.attrs.toString()).getBytes());
					if (clu.condts.lowValue.equals(clu.condts.highValue)) {
						out.write(String.format("%d, min null, max null]\n", clu.condts.lowValue).getBytes());
						continue;
					} else
						out.write("null, min ".getBytes());
					if (clu.condts.lowValue > Integer.MIN_VALUE)
						out.write(clu.condts.lowValue.toString().getBytes());
					else
						out.write("null".getBytes());
					out.write(", max ".getBytes());
					if (clu.condts.highValue < Integer.MAX_VALUE)
						out.write(clu.condts.highValue.toString().getBytes());
					else
						out.write("null".getBytes());
					out.write("]\n".getBytes());
				}
				++layer;
			}
			
			// print scans
			for (Scan scan : joinChildren) {
				out.write(layers, 0, layer);
				
				// print select if necessary
				if (!scan.conditions.isEmpty() || !scan.otherConditions.isEmpty()) {
					
					ArrayList<String> scanConditions = new ArrayList<>();
					
					for (Entry<String, HighLowCondition> condEntry : scan.conditions.entrySet()) {
						if (condEntry.getValue().highValue.equals(condEntry.getValue().lowValue))
							scanConditions.add(condEntry.getKey() + " = " + condEntry.getValue().lowValue);
						else {
							if (condEntry.getValue().highValue < Integer.MAX_VALUE)
								scanConditions.add(condEntry.getKey() + " <= " + condEntry.getValue().highValue);
							if (condEntry.getValue().lowValue > Integer.MIN_VALUE)
								scanConditions.add(condEntry.getKey() + " >= " + condEntry.getValue().lowValue);
						}
					}
					
					for (Condition cond : scan.otherConditions)
						scanConditions.add(cond.toString());
					
					if (!scanConditions.isEmpty()) {
						out.write(String.format("Select[%s]\n", String.join(" AND ", scanConditions)).getBytes());
						out.write(layers, 0, layer + 1);
					}
				}
				
				out.write(String.format("Leaf[%s]\n", scan.fileName).getBytes());
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
