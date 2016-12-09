package physicalPlan;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Vector;

import base.Condition;
import base.DBCatalog;
import logicalPlan.LogPlan;


/**
 * This class is used to optimize a query evaluation plan.
 * 
 * @author Enze Zhou, ez242
 */
public class PhyPlanOptimizer {
	
	private LogPlan logPlan = null;		// Logical plan that is being optimized.
	
	/*
	 * This class contains how to configure a scan operator in optimized plan.
	 */
	public class ScanInfo {
		public String fileName;
		public Integer type;			// 0: full scan. 1: index scan.
		public String keyName;			// if index scan, key name is here. part name.
		
		/**
		 * Constructor of a ScanInfo object.
		 * @param
		 * 		fileName: file name of file being scanned.
		 * 		type: see above.
		 * 		keyName: see above.
		 */
		public ScanInfo(String fileName, Integer type, String keyName) {
			this.fileName = fileName;
			this.type = type;
			this.keyName = keyName;
		}
	}
	public HashMap<String, ScanInfo> finalScanPlan = null;			// Map an alias to a ScanInfo object. Use this to configure physical plan.
	
	
	/*
	 * Intermediate join order for dynamic programming usage.
	 */
	private class JoinOrder {
		HashMap<String, Integer> vValueDict = new HashMap<>();		// V-value dictionary of this operator.
		Vector<Integer> joinOrder = new Vector<>();					// Order of join represented by index in logPlan.joinChildren.
		Integer size;				// Output size of this join operator.
		Integer cost;				// Cost of this sub-join-plan.
		
		public String toString() {
			return String.format("size: %d, cost: %d, order:%s, vValues:%s", size, cost, joinOrder.toString(), vValueDict.toString());
		}
	}
	
	
	public Vector<Integer> finalJoinOrder = null;					// If null: only one relation.
	public Vector<Integer> finalJoinType = null;					// 1: BNLJ. 2: SMJ. If null: only one relation.
	public Vector<Vector<Condition>> finalJoinCond = null;			// Conditions distributed to each join operator.
	
	
	/**
	 * Constructor of optimizer, also main entrance of code.
	 * @param
	 * 		logPlan: the logical plan that is being optimized.
	 */
	public PhyPlanOptimizer(LogPlan logPlan) {
		this.logPlan = logPlan;
		
		planScanType();
		
		if (logPlan.joinChildren.size() > 1) {
			planJoinOrder();
			planJoinType();
		}
	}

	/*
	 * This function plans scan type of every scan.
	 */
	private void planScanType() {
		finalScanPlan = new HashMap<>();
		
		for (LogPlan.Scan scan : logPlan.joinChildren) {
			DBCatalog.RelationInfo relationInfo = DBCatalog.getCatalog().tables.get(scan.fileName);
			Integer totalPage = (int) Math.ceil(relationInfo.tupleNum * relationInfo.attrs.size() * 4.0 / DBCatalog.getCatalog().pageSize);
			
			Integer type = 0;				// Basic case: full scan.
			String keyName = null;
			Integer minCost = totalPage;
			
			// See if using an index could be better.
			for (DBCatalog.IndexInfo index : relationInfo.indexes) {
				DBCatalog.AttrInfo attr = relationInfo.findAttr(index.keyName);
				LogPlan.HighLowCondition cond = scan.conditions.get(scan.alias + '.' + index.keyName);
				
				// If no restriction on this index key, it is strictly worse than full scan so should not be considered.
				if (cond == null || (cond.highValue == Integer.MAX_VALUE && cond.lowValue == Integer.MIN_VALUE))
					continue;
				
				double red = 1.0 * (Math.min(attr.highValue, cond.highValue) - Math.max(attr.lowValue, cond.lowValue) + 1) / (attr.highValue - attr.lowValue + 1);
				Integer cost;
				
				if (index.clustered == 1) {
					cost = (int) (Math.ceil(totalPage * red) + 3);
					
//					System.out.println(String.format("1 red: %f, totalPage: %d, Cost: %d", red, totalPage, cost));
				} else {
					cost = (int) (3 + Math.ceil(relationInfo.tupleNum * red));
					
					// Add leaves read to cost. If leaf count not available in DBCatalog, get it.
					if (index.leafNum == -1) {
						String append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/indexes/" : "db\\indexes\\";
						String indexPath = DBCatalog.getCatalog().inputPath + append + scan.fileName + "." + index.keyName;
						
						try {
							RandomAccessFile indexFile = new RandomAccessFile(indexPath, "r");
							FileChannel indexFC = indexFile.getChannel();
							ByteBuffer BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
							
							indexFC.read(BB);
							BB.flip();
							index.leafNum = BB.getInt(4);
							
							indexFC.close();
							indexFile.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					cost += (int) Math.ceil(index.leafNum * red);
					
//					System.out.println(String.format("0 red: %f, tupleNum: %d, leafNum: %d, Cost: %d", red, relationInfo.tupleNum, index.leafNum, cost));
				}
				
				if (cost < minCost) {
					type = 1;
					keyName = index.keyName;
					minCost = cost;
				}
			}
			
			// Record this plan for scan.
			finalScanPlan.put(scan.alias, new ScanInfo(scan.fileName, type, keyName));
		}
	}
	
	/*
	 * In this project, a set of relations in the plan is represented by a 01 string, s[i] == 1 means logPlan.joinChildren[i] is in this plan.
	 * This class is used to iterate from small subsets to big ones.
	 */
	private class SetStringGenerator {
		private Vector<String> setStrings = new Vector<>();			// All the set strings are generated and stored
																	// when constructing this object, later we will get them one by one.
		private int traverse = 0;									// Next string to give.
		
		/**
		 * Constructor of this class, generate every string in advance.
		 * @param
		 * 		setSize: number of scans.
		 */
		public SetStringGenerator(int setSize) {
			char [] setStringArray = new char [setSize];
			for (int i = 0; i < setSize; ++i)
				setStringArray[i] = '0';
			
			HashSet<String> gotStrings = new HashSet<>();	// Strings that have already be generated.
															// The process below can generate repeatedly so use this to show.
			String tempString = new String(setStringArray);
			gotStrings.add(tempString);
			setStrings.add(tempString);
			
			int i = 0;
			while (i < setStrings.size()) {					// Iterate on the queue until it no longer appends.
				
				// Pick i and generate new ones by modifying one of it's 0s
				setStringArray = setStrings.get(i).toCharArray();
				for (int j = 0; j < setStringArray.length; ++j) {
					if (setStringArray[j] == '1')
						continue;
					setStringArray[j] = '1';
					tempString = new String(setStringArray);
					if (!gotStrings.contains(tempString)) {
						gotStrings.add(tempString);
						setStrings.add(tempString);
					}
					setStringArray[j] = '0';
				}
				++i;
			}
		}
		
		/**
		 * This function return strings one by one.
		 * @return
		 * 		Next set string, from "000..." to ones with 1 element and so on.
		 */
		public String getNextString() {
			if (traverse < setStrings.size())
				return setStrings.get(traverse++);
			else
				return null;
		}
	}
	
	/*
	 * This class generates set string with only one element, because we need to pick plans for single element set from time to time.
	 */
	private class UniSetStringGenerator {
		private char [] uniSetStringCharArray = null;	// All 0. Change one of it to get target string.
		
		/**
		 * Constructor.
		 * @param
		 * 		setSize: number of scans.
		 */
		public UniSetStringGenerator(int setSize) {
			uniSetStringCharArray = new char [setSize];
			for (int i = 0; i < uniSetStringCharArray.length; ++i)
				uniSetStringCharArray[i] = '0';
		}
		
		/**
		 * This function get the set string with only i as the element.
		 * @param
		 * 		The single element in this set.
		 * @return
		 * 		Set string with only i in this set, for example, 1 -> "01000..."
		 */
		public String getUniSetString(int i) {
			uniSetStringCharArray[i] = '1';
			String res = new String(uniSetStringCharArray);
			uniSetStringCharArray[i] = '0';
			return res;
		}
	}
	
	/*
	 * This function plans join order with dynamic programming.
	 */
	private void planJoinOrder() {
		
		// Each set represented by a 01 string is mapped to a joinorder, which is the best plan so far for it.
		HashMap<String, JoinOrder> planMap = new HashMap<>();
		
		SetStringGenerator mySetStringGenerator = new SetStringGenerator(logPlan.joinChildren.size());
		UniSetStringGenerator myUniSetStringGenerator = new UniSetStringGenerator(logPlan.joinChildren.size());
		
		// The first set string is "000..."
		char [] relationSetArray = mySetStringGenerator.getNextString().toCharArray();
		
		// Get all the one-relation plan.
		for (int i = 0; i < relationSetArray.length; ++i) {
			relationSetArray[i] = '1';
			
			LogPlan.Scan scan = logPlan.joinChildren.get(i);
			DBCatalog.RelationInfo relation = DBCatalog.getCatalog().tables.get(scan.fileName);
			
			JoinOrder plan = new JoinOrder();
			plan.joinOrder.add(i);
			plan.cost = 0;
			
			double tempSize = relation.tupleNum;
			for (DBCatalog.AttrInfo attrInfo : relation.attrs) {
				LogPlan.HighLowCondition condition = scan.conditions.get(scan.alias + '.' + attrInfo.name);
				int vValue = attrInfo.highValue - attrInfo.lowValue + 1;
				
				// If there is restriction on this attribute, it's vValue and estimated output size will shrink.
				if (condition != null) {
					vValue = Math.min(attrInfo.highValue, condition.highValue) - Math.max(attrInfo.lowValue, condition.lowValue) + 1;
					tempSize *= (vValue * 1.0 / (attrInfo.highValue - attrInfo.lowValue + 1));
				}
				
				plan.vValueDict.put(scan.alias + '.' + attrInfo.name, vValue);
			}
			
			// If size < 1, it should be 1.
			plan.size = Math.max((int) Math.ceil(tempSize), 1);
			
			// No v-value should be higher than size.
			for (DBCatalog.AttrInfo attrInfo : relation.attrs)
				plan.vValueDict.replace(	scan.alias + '.' + attrInfo.name,
											Math.min(plan.vValueDict.get(scan.alias + '.' + attrInfo.name), plan.size));
			
			planMap.put(new String(relationSetArray), plan);
			relationSetArray[i] = '0';
			
//			System.out.println(String.format("%s size: %d", scan.alias, plan.size));
		}
		
		// Get small sub-plan one by one and build bigger plans by adding one relation to it.
		String relationSetString = null;
		JoinOrder leftPlan = null;
		while ((relationSetString = mySetStringGenerator.getNextString()) != null) {		// When get null, full optimal plan is achieved.
			relationSetArray = relationSetString.toCharArray();
			
			// Building new plans by adding one relation to this sub-plan.
			leftPlan = planMap.get(new String(relationSetArray));
			
			for (int i = 0; i < relationSetArray.length; ++i) {
				if (relationSetArray[i] == '1')		// This relation is already in left sub-plan.
					continue;
				
				relationSetArray[i] = '1';			// Otherwise try adding ith relation to plan.
				
				Integer cost = (leftPlan.joinOrder.size() < 2 ? 0 : leftPlan.cost + leftPlan.size);
				
				// If a better plan already exists in planMap, continue.
				if (planMap.containsKey(new String(relationSetArray)) && planMap.get(new String(relationSetArray)).cost <= cost) {
					relationSetArray[i] = '0';
					continue;
				}
				
				// Start building new plan
				JoinOrder plan = new JoinOrder();
				plan.cost = cost;
				
				// sub-plan for ith relation
				JoinOrder rightPlan = planMap.get(myUniSetStringGenerator.getUniSetString(i));
				
				// If two relations, left should be smaller. Putting the right plan in planMap
				// from the very beginning because later the plan with same cost will be omitted.
				if (leftPlan.joinOrder.size() < 2 && leftPlan.size > rightPlan.size) {
					plan.joinOrder.add(i);
					plan.joinOrder.add(leftPlan.joinOrder.get(0));
				} else {
					for (Integer rela : leftPlan.joinOrder)
						plan.joinOrder.add(rela);
					plan.joinOrder.add(i);
				}
				
				// Put in original v-values from sub-plans.
				plan.vValueDict.putAll(leftPlan.vValueDict);
				plan.vValueDict.putAll(rightPlan.vValueDict);
				
				LogPlan.Cluster attrCluster;
				
				// Get maximum vValue of each union set elements.
				HashMap<LogPlan.Cluster, Integer> vValueOfCluster = new HashMap<>();
				for (Condition cond : logPlan.joinConditions) {
					
					// Non-equal conditions are neglected.
					if (plan.vValueDict.containsKey(cond.leftName) && plan.vValueDict.containsKey(cond.rightName) && cond.operator == Condition.op.e) {
						attrCluster = logPlan.find(cond.leftName);			// leftName and rightName should be in the same union.
						if (!vValueOfCluster.containsKey(attrCluster)) {
							vValueOfCluster.put(attrCluster, Math.max(plan.vValueDict.get(cond.leftName), plan.vValueDict.get(cond.rightName)));
						} else {
							Integer condMaxV = Math.max(plan.vValueDict.get(cond.leftName), plan.vValueDict.get(cond.rightName));
							vValueOfCluster.replace(attrCluster, Math.max(vValueOfCluster.get(attrCluster), condMaxV));
						}
					}
				}
				
				// Join size is cross product divided by vValue of each union.
				double joinSize = leftPlan.size * rightPlan.size;
				for (Integer clusterV : vValueOfCluster.values())
					joinSize /= clusterV;
				plan.size = joinSize > 1 ? (int) Math.ceil(joinSize) : 1;	// Join size will be >= 1
				
				// Get minimum vValue of each union.
				vValueOfCluster.clear();
				for (String attrName : plan.vValueDict.keySet()) {
					attrCluster = logPlan.find(attrName);
					if (vValueOfCluster.containsKey(attrCluster)) {
						vValueOfCluster.replace(attrCluster, Math.min(vValueOfCluster.get(attrCluster), plan.vValueDict.get(attrName)));
					} else {
						vValueOfCluster.put(attrCluster, plan.vValueDict.get(attrName));
					}
				}
				
				// Update vValueDict to set every variable of same union to same vValue
				for (String attrName : plan.vValueDict.keySet()) {
					attrCluster = logPlan.find(attrName);
					
					// vValue should not be bigger than output size but should neither be smaller than 1.
					Integer vValue = Math.min(plan.size, vValueOfCluster.get(attrCluster));
					plan.vValueDict.replace(attrName, Math.max(1, vValue));
				}
				
				/*
				 * Join size is calculated using vValues of children, vValue of join is minimum of children vValues.
				 * Only consider attributed that appear in this join operator, others that will be joined later will not be considered even though they are in the same union.
				 */
				
				planMap.put(new String(relationSetArray), plan);
				relationSetArray[i] = '0';
			}
		}
		
		// In the last iteration, leftPlan will be the plan for whole set.
		finalJoinOrder = leftPlan.joinOrder;
		
		for (Entry<String, JoinOrder> entry : planMap.entrySet())
			System.out.println(String.format("%s partial join plan: %s", entry.getKey(), entry.getValue()));
	}
	
	/*
	 * This function plans join type of every join and get join conditions by the way.
	 */
	private void planJoinType() {
		finalJoinCond = new Vector<>();
		finalJoinType = new Vector<>();
		
		HashSet<String> leftAttrs = new HashSet<>();							// Attributes that in left sub-plan.
		LogPlan.Scan scan = logPlan.joinChildren.get(finalJoinOrder.get(0));	// Left most scan.
		DBCatalog.RelationInfo relation = DBCatalog.getCatalog().tables.get(scan.fileName);
		for (DBCatalog.AttrInfo attr : relation.attrs)
			leftAttrs.add(scan.alias + '.' + attr.name);
		
		for (int i = 1; i < finalJoinOrder.size(); ++i) {
			finalJoinCond.add(new Vector<>());
			
			HashSet<String> rightAttrs = new HashSet<>();
			scan = logPlan.joinChildren.get(finalJoinOrder.get(i));
			relation = DBCatalog.getCatalog().tables.get(scan.fileName);
			for (DBCatalog.AttrInfo attr : relation.attrs)
				rightAttrs.add(scan.alias + '.' + attr.name);
			
			// If find a equality condition, use SMJ. Otherwise, use BNLJ.
			boolean findEqualCond = false;
			for (Condition cond : logPlan.joinConditions) {
				if (leftAttrs.contains(cond.leftName) && rightAttrs.contains(cond.rightName)) {
					finalJoinCond.lastElement().add(cond);
					if (cond.operator == Condition.op.e)
						findEqualCond = true;
				} else if (leftAttrs.contains(cond.rightName) && rightAttrs.contains(cond.leftName)) {
					cond.flip();
					finalJoinCond.lastElement().add(cond);
					if (cond.operator == Condition.op.e)
						findEqualCond = true;
				}
			}
			if (findEqualCond)
				finalJoinType.add(2);
			else
				finalJoinType.add(1);
			
			leftAttrs.addAll(rightAttrs);
		}
		
	}

}
