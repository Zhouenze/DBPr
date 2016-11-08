package base;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/*
 * The class to build B tree index.
 * 
 * @author Enze Zhou ez242
 */
public class BTreeIndex {
	
	private boolean clusteredOnKey;			// Whether the index is clustered on key. Optimization can be applied if so.
	private String fileName;
	private String keyName;
	private int keyId;						// Index of the key.
	private int order;						// Order of this B+ tree.
	
	private ArrayList<Tuple> originalData = null;	// Key-rid data from file. Need to sort it if file is not clustered.
	private FileChannel FC;							// File channel to write index file.

	// Some elements used during the building process.
	private int traverse;
	private int childLayerPgNumMin;
	
	/*
	 * Constructor, also the main workload.
	 * @param
	 * 		fileName: the file whose index is being built.
	 * 		keyName: the key of the index.
	 * 		clusteredOnKey: whether the file is clustered on key.
	 * 		order: the order of the tree.
	 */
	public BTreeIndex(String fileName, String keyName, boolean clusteredOnKey, int order) {
		this.fileName = fileName;
		this.keyName = keyName;
		this.keyId = DBCatalog.getCatalog().tables.get(fileName).indexOf(keyName);
		this.clusteredOnKey = clusteredOnKey;
		this.order = order;
		
		// build standard index.
		buildIndex();
		
		// Readable index in the same directory if needed.
		readableIndex();
	}
	
	/*
	 * Build the index.
	 */
	private void buildIndex() {
		readOriginalData();
		
		// The B tree is stored here. Every element of the tree is a layer from bottom to top. Build process from bottom to top.
		ArrayList<ArrayList<BTreeNode>> tree = new ArrayList<>();
		
		String append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/indexes/" : "db\\indexes\\";
		RandomAccessFile FOS = null;
		try {
			// Index file to write to.
			FOS = new RandomAccessFile(DBCatalog.getCatalog().inputPath + append + fileName + "." + keyName, "rw");
			FC = FOS.getChannel();
			
			// Write nothing to the header page to skip it. Later we will write this page again.
			ByteBuffer BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
			BB.clear();
			FC.write(BB);
			BB.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		{	// Build leaves.
			int keyCount = 0;								// Number of different keys. Used to calculate leaf node number.
			int lastKey = Integer.MIN_VALUE;
			for (int i = 0; i < originalData.size(); ++i)
				if (originalData.get(i).data.get(0) != lastKey) {
					++keyCount;
					lastKey = originalData.get(i).data.get(0);
				}
			
			// full: number of full leaves to build.
			// secondLast: number of keys in second to last leaf if last two leaves need to be balanced. 0 if not.
			// last: number of keys in the last leaf. 0 if every leaf is full.
			int secondLast = 0;
			int full = (keyCount / (2 * order));
			int last = (keyCount % (2 * order));
			if (last > 0 && last < order && full >= 1) {
				--full;
				secondLast = (last + 2 * order) / 2;
				last = last + 2 * order - secondLast;
			}
			
			tree.add(new ArrayList<BTreeNode>());
			traverse = 0;
			
			// Build all the leaves.
			for(int i = 0; i < full; ++i)
				tree.get(0).add(new BTreeLeafNode(2 * order));
			if (secondLast > 0)
				tree.get(0).add(new BTreeLeafNode(secondLast));
			if (last > 0)
				tree.get(0).add(new BTreeLeafNode(last));
			
			// Write all the leaves to file.
			for (int i = 0; i < tree.get(0).size(); ++i)
				tree.get(0).get(i).writeToFile();
		}
		
		// Build non-root index layers.
		int layer = 1;
		childLayerPgNumMin = 1;		// Smallest page address of child layer. Needed when write index file.
		while(true) {
			int lastSize = tree.get(layer - 1).size();
			if (lastSize <= 2 * order + 1)					// This layer has only one node. So break and build the root.
				break;
			
			// Same as before.
			int secondLast = 0;
			int full = lastSize / (2 * order + 1);
			int last = lastSize % (2 * order + 1);
			if (last > 0 && last < order + 1 && full >= 1) {
				--full;
				secondLast = (last + 2 * order + 1) / 2;
				last = last + 2 * order + 1 - secondLast;
			}
			
			tree.add(new ArrayList<BTreeNode>());
			traverse = 0;
			
			// Build all the index nodes in this layer and write them to file.
			for(int i = 0; i < full; ++i) {
				tree.get(layer).add(new BTreeIndexNode(2 * order + 1, tree.get(layer - 1)));
				tree.get(layer).get(tree.get(layer).size() - 1).writeToFile();
			}
			if (secondLast > 0) {
				tree.get(layer).add(new BTreeIndexNode(secondLast, tree.get(layer - 1)));
				tree.get(layer).get(tree.get(layer).size() - 1).writeToFile();
			}
			if (last > 0) {
				tree.get(layer).add(new BTreeIndexNode(last, tree.get(layer - 1)));
				tree.get(layer).get(tree.get(layer).size() - 1).writeToFile();
			}
			
			// Modify it to the start page address of this new layer.
			childLayerPgNumMin += tree.get(layer - 1).size();
			++layer;
		}
		
		// Build root.
		tree.add(new ArrayList<BTreeNode>());
		traverse = 0;
		tree.get(layer).add(new BTreeIndexNode(tree.get(layer - 1).size(), tree.get(layer - 1)));
		tree.get(layer).get(0).writeToFile();
		childLayerPgNumMin += tree.get(layer - 1).size();	// Now it's the address of root.
		
		// Write header.
		try {
			FOS.seek(0);
			
			ByteBuffer BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
			BB.putInt(0, childLayerPgNumMin);
			BB.putInt(4, tree.get(0).size());
			BB.putInt(8, order);
			
			for (int i = 12; i < BB.capacity(); ++i)
				BB.put(i, (byte) 0);
			BB.clear();
			FC.write(BB);
			BB.clear();
			
			FC.close();
			FOS.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Read an existing index and rewrite to a human-readable form like the one provided.
	 */
	public void readableIndex() {
		String append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/indexes/" : "db\\indexes\\";
		try {
			RandomAccessFile inputFile = new RandomAccessFile(DBCatalog.getCatalog().inputPath + append + fileName + "." + keyName, "r");
			PrintWriter outputFile = new PrintWriter(DBCatalog.getCatalog().inputPath + append + fileName + "." + keyName + "_humanreadable");
			FileChannel inputChannel = inputFile.getChannel();
			ByteBuffer inputBuffer = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
			
			inputChannel.read(inputBuffer);
			inputBuffer.flip();
			int rootAddr = inputBuffer.getInt(0);
			int leavesNum = inputBuffer.getInt(4);
			int order = inputBuffer.getInt(8);
			
			// Write header information.
			String content = "Header Page info: tree has order " + order + ", a root at address " + rootAddr + " and " + leavesNum + " leaf nodes \n";
			outputFile.println(content);
			
			ArrayList<Integer> children = new ArrayList<>();			// Current layer.
			ArrayList<Integer> newChildren = null;						// All the children of current layer nodes.
			int childAddr;
			
			// Go to root and write information.
			inputFile.seek(DBCatalog.getCatalog().pageSize * rootAddr);
			inputChannel.read(inputBuffer);
			inputBuffer.flip();
			
			int keyCount = inputBuffer.getInt(4);
			content = "Root node is: IndexNode with keys [";
			if (keyCount > 0)										// Root can have 0 key so this need to be handled.
				content += String.valueOf(inputBuffer.getInt(8));
			for (int i = 1; i < keyCount; ++i)
				content += (", " + inputBuffer.getInt(8 + i * 4));
			childAddr = inputBuffer.getInt(8 + keyCount * 4);
			content += ("] and child addresses [" + childAddr);		// Root must have at least one child so this doesn't need to be handled separately.
			children.add(childAddr);
			for (int i = 1; i < keyCount + 1; ++i) {
				childAddr = inputBuffer.getInt(8 + keyCount * 4 + i * 4);
				content += (", " + childAddr);
				children.add(childAddr);
			}
			content += "]\n";
			outputFile.println(content);

			// Write other index layers.
			while (children.get(0) > leavesNum) {
				newChildren = new ArrayList<>();
				
				outputFile.println("---------Next layer is index nodes---------");
				for (int i = 0; i < children.size(); ++i) {						// Here children means all the index nodes in this layer that are being written.
					inputFile.seek(DBCatalog.getCatalog().pageSize * children.get(i));
					inputChannel.read(inputBuffer);
					inputBuffer.flip();
					
					content = "IndexNode with keys [" + inputBuffer.getInt(8);	// Every index node other than root has at least one key.
					keyCount = inputBuffer.getInt(4);
					for (int j = 1; j < keyCount; ++j)
						content += (", " + inputBuffer.getInt(8 + j * 4));
					childAddr = inputBuffer.getInt(8 + keyCount * 4);
					content += ("] and child addresses [" + childAddr);
					newChildren.add(childAddr);
					for (int j = 1; j < keyCount + 1; ++j) {
						childAddr = inputBuffer.getInt(8 + keyCount * 4 + j * 4);
						content += (", " + childAddr);
						newChildren.add(childAddr);								// All the nodes in next layer.
					}
					content += "]\n";
					outputFile.println(content);
				}
				children = newChildren;
			}
			
			// Write all the leaf nodes.
			outputFile.println("---------Next layer is leaf nodes---------");
			for (int i = 0; i < leavesNum; ++i) {
				outputFile.println("LeafNode[");
				inputFile.seek(DBCatalog.getCatalog().pageSize * (i + 1));
				inputChannel.read(inputBuffer);
				inputBuffer.flip();
				
				keyCount = inputBuffer.getInt(4);
				int pos = 8;
				for (int j = 0; j < keyCount; ++j) {
					content = "<[" + inputBuffer.getInt(pos) + ":";
					for (int k = 0; k < inputBuffer.getInt(pos + 4); ++k) {
						content += ("(" + inputBuffer.getInt(pos + 8 + k * 8) + "," + inputBuffer.getInt(pos + 12 + k * 8) + ")");
					}
					content += "]>";
					outputFile.println(content);
					
					pos += (8 + inputBuffer.getInt(pos + 4) * 8);
				}
				
				outputFile.println("]\n");
			}
			
			
			inputFile.close();
			outputFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * This function reads original data and sort it if not clustered.
	 */
	private void readOriginalData() {
		originalData = new ArrayList<>();
		try {
			String append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/data/" : "db\\data\\";
			TupleReader TR = new TupleReader(DBCatalog.getCatalog().inputPath + append + fileName);
			Tuple tp;
			while ((tp = TR.getNextKeyRid(keyId)) != null) {
				originalData.add(tp);
			}
			TR.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		if (!clusteredOnKey)
			Collections.sort(originalData, new keyRidComparator());
	}
	
	/*
	 * Base class of B tree node.
	 * Every node has a key list and they need to have a method to write to file.
	 */
	private abstract class BTreeNode {
		ArrayList<Integer> keys = new ArrayList<>();
		
		abstract void writeToFile();					// Write the information of this node to file according to format.
	}
	
	/*
	 * This class is index nodes.
	 * @superclass	BTreeNode
	 */
	private final class BTreeIndexNode extends BTreeNode {
		ArrayList<BTreeNode> children = new ArrayList<>();	// Index nodes have children list.
		
		/*
		 * Constructor of index nodes.
		 * @param
		 * 		childNum: how many children does this index node have.
		 * 		childLayer: reference to child layer.
		 */
		public BTreeIndexNode(int childNum, ArrayList<BTreeNode> childLayer) {
			super();
			
			// All the index nodes in this layer share this traverse value so that when a particular node is built, traverse points to the beginning of next child.
			for (int i = 0; i < childNum; ++i) {
				children.add(childLayer.get(traverse));
				++traverse;
			}
			
			// Get all the keys.
			for (int i = 1; i < childNum; ++i) {
				BTreeNode node = children.get(i);
				while (node instanceof BTreeIndexNode)
					node = ((BTreeIndexNode)node).children.get(0);
				keys.add(node.keys.get(0));
			}
		}

		/*
		 * Write this index node to file according to format.
		 * @override from super class BTreeNode.
		 */
		@Override
		void writeToFile() {
			ByteBuffer BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
			BB.putInt(0, 1);
			BB.putInt(4, keys.size());
			int writePos = 8;
			
			// Write all the keys.
			for (int i = 0; i < keys.size(); ++i) {
				BB.putInt(writePos, keys.get(i));
				writePos += 4;
			}
			
			// For index nodes, writeToFile is called immediately after construction so traverse is end of children.
			traverse -= children.size();
			for (int i = 0; i < children.size(); ++i) {
				BB.putInt(writePos, childLayerPgNumMin + traverse);
				writePos += 4;
				++traverse;
			}
			
			for (int i = writePos; i < BB.capacity(); ++i)
				BB.put(i, (byte) 0);
			BB.clear();
			try {
				FC.write(BB);
			} catch (IOException e) {
				e.printStackTrace();
			}
			BB.clear();
		}
		
	}
	
	/*
	 * This class is leaf nodes.
	 * @superclass BTreeNode
	 */
	private final class BTreeLeafNode extends BTreeNode {
		ArrayList<ArrayList<Tuple>> rids = new ArrayList<>();		// For leaf nodes, every key has a list of rids.
		
		/*
		 * Constructor of leaf nodes.
		 * @param
		 * 		keyNum: how many keys does this leaf node have.
		 */
		public BTreeLeafNode(int keyNum) {
			int tempKeyNum = 0;
			while (tempKeyNum < keyNum) {
				int key = originalData.get(traverse).data.get(0);
				keys.add(key);
				rids.add(new ArrayList<Tuple>());
				
				// Unlike for index node whose count is pre-assigned, here we need a flag to stop getting next data, so out-of-index need to be handled.
				while (traverse < originalData.size() && originalData.get(traverse).data.get(0) == key) {
					Tuple rid = new Tuple();
					rid.data.add(originalData.get(traverse).data.get(1));
					rid.data.add(originalData.get(traverse).data.get(2));
					rids.get(rids.size() - 1).add(rid);
					++traverse;
				}
				++tempKeyNum;
			}
		}

		/*
		 * Write this leaf node to file according to format.
		 * @override from super class BTreeNode.
		 */
		@Override
		void writeToFile() {
			ByteBuffer BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
			BB.putInt(0, 0);
			BB.putInt(4, keys.size());
			int writePos = 8;
			for (int i = 0; i < keys.size(); ++i) {
				BB.putInt(writePos, keys.get(i));
				writePos += 4;
				BB.putInt(writePos, rids.get(i).size());
				writePos += 4;
				for (int j = 0; j < rids.get(i).size(); ++j) {
					BB.putInt(writePos, rids.get(i).get(j).data.get(0));
					writePos += 4;
					BB.putInt(writePos, rids.get(i).get(j).data.get(1));
					writePos += 4;
				}
			}
			for (int i = writePos; i < BB.capacity(); ++i)
				BB.put(i, (byte) 0);
			BB.clear();
			try {
				FC.write(BB);
			} catch (IOException e) {
				e.printStackTrace();
			}
			BB.clear();
		}
		
	}
	
	/*
	 * Customized Comparator to compare tuples. Actually only need to compare on key but key is at the beginning of the tuple and comparing all of them would be better.
	 */
	private class keyRidComparator implements Comparator<Tuple> {
		
		/*
		 * compare two tuples according to their data.
		 * @params
		 * 		t1, t2: two tuples being compared.
		 * @return
		 * 		standard compare return value integer.
		 */
		public int compare(Tuple t1, Tuple t2) {
			for (int j = 0; j < 3; ++j) {
				int ret = t1.data.get(j).compareTo(t2.data.get(j));
				if (ret != 0)
					return ret;
			}
			return 0;
		}
	}
	
}
