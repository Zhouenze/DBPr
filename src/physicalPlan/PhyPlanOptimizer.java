package physicalPlan;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import base.Condition;
import base.DBCatalog;
import logicalPlan.LogPlan;

public class PhyPlanOptimizer {
	
	private LogPlan logPlan = null;
	
	public class ScanInfo {
		public String fileName;
		public Integer type;
		public String keyName;
		
		public ScanInfo(String fileName, Integer type, String keyName) {
			this.fileName = fileName;
			this.type = type;
			this.keyName = keyName;
		}
	}
	public HashMap<String, ScanInfo> scanPlan = null;
	
	
	private class JoinOrder {
		HashMap<String, Integer> vValueDict = new HashMap<>();
		Vector<Integer> joinOrder = new Vector<>();
		Integer size;
		Integer cost;
	}
	
	
	public Vector<Integer> finalJoinOrder = null;
	public Vector<Integer> finalJoinType = null;
	public Vector<Vector<Condition>> finalJoinCond = null;
	
	
	public PhyPlanOptimizer(LogPlan logPlan) {
		this.logPlan = logPlan;
		
		planScanType();
		
		if (logPlan.joinChildren.size() > 1) {
			planJoinOrder();
			planJoinType();
		}
	}

	private void planScanType() {
		scanPlan = new HashMap<>();
		
		for (LogPlan.Scan scan : logPlan.joinChildren) {
			DBCatalog.RelationInfo relationInfo = DBCatalog.getCatalog().tables.get(scan.fileName);
			Integer totalPage = (int) Math.ceil(relationInfo.tupleNum * relationInfo.attrs.size() / 1024.0);
			
			Integer type = 0;
			String keyName = null;
			Integer minCost = totalPage;
			
			for (DBCatalog.IndexInfo index : relationInfo.indexes) {
				DBCatalog.AttrInfo attr = relationInfo.attrs.get(relationInfo.findIdOfAttr(index.keyName));
				int pushedCondId = scan.findIdOfPushedCond(index.keyName);
				if (pushedCondId < 0)
					continue;
				LogPlan.PushedConditions cond = scan.conditions.get(pushedCondId);
				
				double red = 1.0 * (Math.min(attr.highValue, cond.highValue) - Math.max(attr.lowValue, cond.lowValue) + 1) / (attr.highValue - attr.lowValue + 1);
				Integer cost;
				
				if (index.clustered == 1) {
					cost = (int) (totalPage * red + 3);
				} else {
					cost = (int) (3 + relationInfo.tupleNum * red);
					
					String append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/indexes/" : "db\\indexes\\";
					String indexPath = DBCatalog.getCatalog().inputPath + append + scan.fileName + "." + index.keyName;
					
					try {
						RandomAccessFile indexFile = new RandomAccessFile(indexPath, "r");
						FileChannel indexFC = indexFile.getChannel();
						ByteBuffer BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
						
						// Read header information.
						indexFC.read(BB);
						BB.flip();
						cost += (int) (BB.getInt(4) * red);
						
						indexFC.close();
						indexFile.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				if (cost < minCost) {
					type = 1;
					keyName = index.keyName;
					minCost = cost;
				}
			}
			scanPlan.put(scan.alias, new ScanInfo(scan.fileName, type, keyName));
		}
	}
	
	private class SetStringGenerator {
		Vector<String> setStrings = new Vector<>();
		int traverse = 0;
		
		public SetStringGenerator(int setSize) {
			char [] setStringArray = new char [setSize];
			for (int i = 0; i < setSize; ++i)
				setStringArray[i] = '0';
			
			HashSet<String> gotStrings = new HashSet<>();
			String tempString = setStringArray.toString();
			gotStrings.add(tempString);
			setStrings.add(tempString);
			
			int i = 0;
			while (i < setStrings.size()) {
				setStringArray = setStrings.get(i).toCharArray();
				for (int j = 0; j < setStringArray.length; ++j) {
					if (setStringArray[j] == '1')
						continue;
					setStringArray[j] = '1';
					tempString = setStringArray.toString();
					if (!gotStrings.contains(tempString)) {
						gotStrings.add(tempString);
						setStrings.add(tempString);
					}
					setStringArray[j] = '0';
				}
				++i;
			}
		}
		
		public String getNextString() {
			if (traverse < setStrings.size())
				return setStrings.get(traverse++);
			else
				return null;
		}
	}
	
	private SetStringGenerator mySetStringGenerator = null;
	
	private class UniSetStringGenerator {
		char [] uniSetStringCharArray = null;
		
		public UniSetStringGenerator(int setSize) {
			uniSetStringCharArray = new char [setSize];
			for (int i = 0; i < setSize; ++i)
				uniSetStringCharArray[i] = '0';
		}
		
		public String getUniSetString(int i) {
			uniSetStringCharArray[i] = '1';
			String res = uniSetStringCharArray.toString();
			uniSetStringCharArray[i] = '0';
			return res;
		}
	}
	
	private UniSetStringGenerator myUniSetStringGenerator = null;
	
	private void planJoinOrder() {
		HashMap<String, JoinOrder> planMap = new HashMap<>();
		
		mySetStringGenerator = new SetStringGenerator(logPlan.joinChildren.size());
		myUniSetStringGenerator = new UniSetStringGenerator(logPlan.joinChildren.size());
		
		char [] relationSetArray = mySetStringGenerator.getNextString().toCharArray();
		
		for (int i = 0; i < relationSetArray.length; ++i) {
			relationSetArray[i] = '1';
			
			LogPlan.Scan scan = logPlan.joinChildren.get(i);
			DBCatalog.RelationInfo relation = DBCatalog.getCatalog().tables.get(scan.fileName);
			
			JoinOrder joinOrder = new JoinOrder();
			joinOrder.joinOrder.add(i);
			joinOrder.cost = 0;
			
			double tempSize = relation.tupleNum;
			for (DBCatalog.AttrInfo attrInfo : relation.attrs) {
				int pushedCondId = scan.findIdOfPushedCond(attrInfo.name);
				LogPlan.PushedConditions condition = null;
				if (pushedCondId >= 0)
					condition = scan.conditions.get(pushedCondId);
				int vValue = attrInfo.highValue - attrInfo.lowValue + 1;
				if (condition != null) {
					vValue = Math.min(vValue, Math.min(attrInfo.highValue, condition.highValue) - Math.max(attrInfo.lowValue, condition.lowValue) + 1);
					tempSize *= ((Math.min(attrInfo.highValue, condition.highValue) - Math.max(attrInfo.lowValue, condition.lowValue)) * 1.0 / (attrInfo.highValue - attrInfo.lowValue));
				}
				joinOrder.vValueDict.put(scan.alias + '.' + attrInfo.name, vValue);
			}
			joinOrder.size = Math.max((int) Math.ceil(tempSize), 1);
			for (DBCatalog.AttrInfo attrInfo : relation.attrs) {
				joinOrder.vValueDict.replace(attrInfo.name, Math.min(joinOrder.vValueDict.get(attrInfo.name), joinOrder.size));
			}
			
			planMap.put(relationSetArray.toString(), joinOrder);
			relationSetArray[i] = '0';
		}
		
		String relationSetString = null;
		while ((relationSetString = mySetStringGenerator.getNextString()) != null) {
			relationSetArray = relationSetString.toCharArray();
			JoinOrder leftPlan = planMap.get(relationSetArray.toString());
			
			for (int i = 0; i < relationSetArray.length; ++i) {
				if (relationSetArray[i] == '1')
					continue;
				relationSetArray[i] = '1';
				
				Integer cost = (leftPlan.joinOrder.size() < 2 ? 0 : leftPlan.cost + leftPlan.size);
				if (planMap.containsKey(relationSetArray.toString()) && planMap.get(relationSetArray.toString()).cost <= cost) {
					relationSetArray[i] = '0';
					continue;
				}
				JoinOrder plan = new JoinOrder();
				plan.cost = cost;
				
				for (Integer rela : leftPlan.joinOrder)
					plan.joinOrder.add(rela);
				plan.joinOrder.add(i);
				
				JoinOrder rightPlan = planMap.get(myUniSetStringGenerator.getUniSetString(i));
				for (String attr : leftPlan.vValueDict.keySet())
					plan.vValueDict.put(attr, leftPlan.vValueDict.get(attr));
				for (String attr : rightPlan.vValueDict.keySet())
					plan.vValueDict.put(attr, rightPlan.vValueDict.get(attr));
				
				
				
				
				planMap.put(relationSetArray.toString(), plan);
				relationSetArray[i] = '0';
			}
		}
		
		
		
		
	}
	
	private void planJoinType() {
		finalJoinCond = new Vector<>();
		finalJoinType = new Vector<>();
		
		HashSet<String> leftAttrs = new HashSet<>();
		LogPlan.Scan scan = logPlan.joinChildren.get(finalJoinOrder.get(0));
		DBCatalog.RelationInfo relation = DBCatalog.getCatalog().tables.get(scan.fileName);
		for (DBCatalog.AttrInfo attr : relation.attrs) {
			leftAttrs.add(scan.alias + '.' + attr.name);
		}
		
		for (int i = 1; i < finalJoinOrder.size(); ++i) {
			finalJoinCond.add(new Vector<>());
			
			HashSet<String> rightAttrs = new HashSet<>();
			scan = logPlan.joinChildren.get(finalJoinOrder.get(i));
			relation = DBCatalog.getCatalog().tables.get(scan.fileName);
			for (DBCatalog.AttrInfo attr : relation.attrs) {
				rightAttrs.add(scan.alias + '.' + attr.name);
			}
			
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
