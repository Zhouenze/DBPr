package physicalPlan;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.Math;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import base.Condition;
import base.DBCatalog;
import base.Tuple;
import base.TupleReader;

/*
 * This class is index scan operator.
 * @superclass PhyScanOp
 * 
 * @author Enze Zhou ez242
 */
public class PhyScanIndexOp extends PhyScanOp {
	
	private String indexPath = null;
	private String keyName = null;
	private int keyId = -1;						// ID of key in data.
	private int lowKey = Integer.MIN_VALUE;		// They are set to this value so that if no condition is given, all the key value is valid.
	private int highKey = Integer.MAX_VALUE;	// The valid range is [lowKey, highKey].
	private boolean [] validConditions;			// Whether the corresponding condition is valid or has been included in high-low kye.
	private boolean clustered;
	private int rootAddr;						// Root page address in index file.
	private int leavesNum;						// Number of leaves in index file.
	
	// Things to read index file.
	private RandomAccessFile indexFile = null;
	private FileChannel indexFC = null;
	private ByteBuffer BB = null;
	
	// Things to read data file.
	private TupleReader dataFile1 = null;		// If clustered, traverse this one for better performance.
	private RandomAccessFile dataFile2 = null;	// If unclustered, use random access to jump and read next tuple.
	
	// Things to find the next tuple rid that satisfies high-low key according to index. It doesn't necessarily satisfy other conditions so further judgment is needed.
	private int BBPageAddr;			// Corresponding page address of BB.
	private int nextEntryStart;		// Start position of the entry in BB where next valid tuple rid is in.
	private int nextEntryId;		// ID of the entry in BB where next valid tuple rid is in.
	private int nextRidStart;		// Start position of the next valid tuple rid in BB. If this is set to -1, there is no valid tuples anymore. The output of this operator ends.
	private int nextRidId;			// ID of the next valid tuple rid in corresponding entry.
	
	/*
	 * Constructor.
	 * @param
	 * 		fileName: file being scanned by this index scan operator.
	 * 		alias: alias of this operator in query.
	 */
	public PhyScanIndexOp(String fileName, String alias) {
		super();
		this.fileName = fileName;
		this.alias = alias;
		keyName = DBCatalog.getCatalog().tables.get(fileName).indexes.get(0).keyName;
		keyId = DBCatalog.getCatalog().tables.get(fileName).findIdOfAttr(keyName);
		clustered = (DBCatalog.getCatalog().tables.get(fileName).indexes.get(0).clustered == 1);
		String append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/indexes/" : "db\\indexes\\";
		indexPath = DBCatalog.getCatalog().inputPath + append + fileName + "." + keyName;
		
		try {
			indexFile = new RandomAccessFile(indexPath, "r");
			indexFC = indexFile.getChannel();
			BB = ByteBuffer.allocate(DBCatalog.getCatalog().pageSize);
			
			append = DBCatalog.getCatalog().inputPath.contains("/") ? "db/data/" : "db\\data\\";
			if (this.clustered) {
				dataFile1 = new TupleReader(DBCatalog.getCatalog().inputPath + append + fileName);
			} else {
				dataFile2 = new RandomAccessFile(DBCatalog.getCatalog().inputPath + append + fileName, "r");
			}
			
			// Read header information.
			indexFC.read(BB);
			BB.flip();
			rootAddr = BB.getInt(0);
			leavesNum = BB.getInt(4);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Function to initialize this operator.
	 * This function need to be called when:
	 * 		1. Index is built.
	 * 		2. Conditions that should be handled by this operator is distributed.
	 * It separates the conditions that are handled by index and others and seek next valid rid in index.
	 * @return
	 * 		whether there is a valid rid according to index.
	 */
	public boolean initialize() {
		validConditions = new boolean [conditions.size()];
		
		// If a condition has one element and a constant, constant is on the right. This is handled by constructor of Condition.
		for (int i = 0; i < conditions.size(); ++i) {
			Condition cond = conditions.get(i);
			
			// Only these conditions can be expressed by high-low key.
			if (cond.leftName.equals(alias + "." + keyName) && cond.rightName == null && cond.operator != Condition.op.ne) {
				
				switch (cond.operator) {
				case l:
					highKey = Math.min(highKey, cond.right - 1);
					break;
				case g:
					lowKey = Math.max(lowKey, cond.right + 1);
					break;
				case e:
					// If this condition consists with previous ones, set it. Otherwise, set high-low key to impossible value.
					if (lowKey <= cond.right && highKey >= cond.right) {
						lowKey = highKey = cond.right;
					} else {
						lowKey = 1;
						highKey = 0;
					}
					break;
				case le:
					highKey = Math.min(highKey, cond.right);
					break;
				case ge:
					lowKey = Math.max(lowKey, cond.right);
					break;
				default:
					break;
				}
				
				// Record whether this consitions is covered by high-low key.
				validConditions[i] = false;
			} else {
				validConditions[i] = true;
			}
		}
		
		// Seek to first valid rid in index.
		return seekToFirstRid();
	}
	
	/*
	 * This function seeks to the first valid rid according to high-low key and the index.
	 * @return
	 * 		whether this rid exists.
	 */
	private boolean seekToFirstRid() {
		if (highKey < lowKey) {
			nextRidStart = -1;			// Set end flag.
			return false;
		}
		
		try {
			int addr = rootAddr;	// The node being evaluated.
			int keyNum;
			int childStart;
			
			// Go through all the index nodes according to lowKey.
			while (addr > leavesNum) {
				indexFile.seek(DBCatalog.getCatalog().pageSize * addr);
				indexFC.read(BB);
				BB.flip();
				
				keyNum = BB.getInt(4);
				childStart = 8 + keyNum * 4;
				int childId = 0;
				for (int i = 0; i < keyNum; ++i) {
					if (BB.getInt(8 + i * 4) <= lowKey)
						++childId;
					else
						break;
				}
				addr = BB.getInt(childStart + childId * 4);
			}
			
			// If lowKey exists in tree, it's in the leaf node assigned by addr now. But if it doesn't, going through following leaves to find the one satisfying lowKey.
			indexFile.seek(DBCatalog.getCatalog().pageSize * addr);
			while (addr <= leavesNum) {
				// Read sequentially so no need to seek.
				indexFC.read(BB);
				BB.flip();
				BBPageAddr = addr;
				int entryNum = BB.getInt(4);
				int entryStart = 8;
				for (int i = 0; i < entryNum; ++i) {
					if (BB.getInt(entryStart) > highKey) {	// No valid in index. End flag and return.
						nextRidStart = -1;
						return false;
					}
					if (BB.getInt(entryStart) >= lowKey) {	// Find it.
						nextEntryStart = entryStart;
						nextEntryId = i;
						nextRidStart = entryStart + 8;
						nextRidId = 0;
						
						if (clustered) {					// For clustered index, jump to data page so that later 
															// we can just read tuples one by one from clustered data file,
															// no need to see index anymore.
							// Currently on page 0. If we plan to go to page 6, jump 6 pages.
							dataFile1.jumpPages(BB.getInt(nextRidStart));
							// Next is tuple 0. If we want next to be tuple 6, jump six tuples.
							for (int j = 0; j < BB.getInt(nextRidStart + 4); ++j)
								dataFile1.getNextTuple();
						}
						return true;
					}
					entryStart += (8 + BB.getInt(entryStart + 4) * 8);
				}
				++addr;
			}
			nextRidStart = -1;	// Not found. Set end flag and return.
			return false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * Method that return next tuple in the output of this node.
	 * @override from super class PhyOp
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		if (lowKey > highKey || nextRidStart == -1)
			return null;
		
		// For clustered data file, get next one until it satisfies both high-low key and other conditions or come to an end.
		if (clustered) {
			Tuple tp = null;
			try {
				while ((tp = dataFile1.getNextTuple()) != null) {
					if (tp.data.get(keyId) > highKey) {
						nextRidStart = -1;				// End here.
						return null;
					}
					boolean valid = true;
					for (int i = 0; i < conditions.size(); ++i) {
						if (validConditions[i] && !conditions.get(i).test(tp, schema)) {
							valid = false;
							break;
						}
					}
					if (valid)							// Satisfies other conditions.
						return tp;
				}
				nextRidStart = -1;						// Data file is over. End here.
				return null;
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			try {
				while (true) {							// Rid points to next one satisfying high-low key. Go through them until find one that can also satisfy other conditions.
					if (nextRidStart == -1) {
						return null;
					}
					
					// Get current one according to rid. Use random access file to go there.
					int pageId = BB.getInt(nextRidStart);
					int tupleId = BB.getInt(nextRidStart + 4);
					dataFile2.seek(pageId * DBCatalog.getCatalog().pageSize + 8 + tupleId * schema.size() * 4);
					Tuple tp = new Tuple();
					for (int i = 0; i < schema.size(); ++i)
						tp.data.add(dataFile2.readInt());
					
					// Find next valid rid.
					if (nextRidId + 1 < BB.getInt(nextEntryStart + 4)) {			// More rid in this entry. Surely valid because same key.
						++nextRidId;
						nextRidStart += 8;
					} else if (nextEntryId + 1 < BB.getInt(4)) {					// Entry is over but this leaf has more entry.
						++nextEntryId;
						nextEntryStart += (8 + BB.getInt(nextEntryStart + 4) * 8);
						nextRidId = 0;
						nextRidStart = nextEntryStart + 8;
						
						if (BB.getInt(nextEntryStart) > highKey)					// The key may be over. There is no next one but current one may still be fine so don't return here.
																					// If current one fails, will return on next loop on line 257
							nextRidStart = -1;
					} else if (BBPageAddr + 1 <= leavesNum) {						// Leaf page is over but has more leaf pages.
						indexFC.read(BB);											// Sequential read so no need to seek.
						BB.flip();
						
						++BBPageAddr;
						nextEntryId = 0;
						nextEntryStart = 8;
						nextRidId = 0;
						nextRidStart = nextEntryStart + 8;
						
						if (BB.getInt(nextEntryStart) > highKey)					// The key may be over. Same as above.
							nextRidStart = -1;
					} else {														// Index is over.
						nextRidStart = -1;
					}
					
					boolean valid = true;
					for (int i = 0; i < conditions.size(); ++i) {
						if (validConditions[i] && !conditions.get(i).test(tp, schema)) {
							valid = false;
							break;
						}
					}
					if (valid)														// Satisfy other conditions.
						return tp;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}

	/*
	 * Method that reset output of this node to the beginning. Return to status just after construction.
	 * @override from super class PhyOp
	 */
	@Override
	public void reset() {
		try {
			indexFile.seek(DBCatalog.getCatalog().pageSize);	// Read one page in constructor so return to second page.
			
			// If clustered, reset tuple reader
			if (clustered) {
				dataFile1.reset();
			}
			// If unclustered, a random access file is used after seeking every time so no need to reset.
			
			// Get first valid rid according to index.
			seekToFirstRid();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
