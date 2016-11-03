package physicalPlan;
import java.util.LinkedList;
import java.util.List;

import base.Condition;
import base.Tuple;

/*
 * Brute force implementation of join operator
 * Operator that joins the outputs of its two children, both are Scan Operators inferred by Operator pointer.
 * Inherited from PhyCondOp to have a conditions vector that is used to filter the output tuples of this node.
 * 
 * @authors Enze Zhou ez242, Shuang Zhang sz468, Weicheng Yu wy248
 */
public class PhyJoinBNLJOp extends PhyJoinOp {
	
//	public TupleReader tupleReader = null;
//	public String leftFileName;
	private int attrCounts;
	private int bufferSizeInPages;
	private int tuplesPerBlock;
	private List<Tuple> tupleBlock;
	private int innerIndex = 0;
	private boolean blockStarted = false;
	

	
	
	public Tuple rightTuple = null;	// The right tuple now. This need to be an element of class
								// because it should keep between different calls to getNextTuple().
	boolean end = false;		// denote whether this node has already be fully got.

	/*
	 * Constructor for BNLJ
	 * @param bufferPages specify how many pages a block uses
	 * Note the constructor assumes that you do not have knowledge about files
	 * to be read and # of attributes in the files
	 * 
	 * Need to call setleftfile for tuplereader to work properly
	 * so that tuple readers can be used
	 */
	public PhyJoinBNLJOp(int bufferPages) {
		bufferSizeInPages = bufferPages;
		tupleBlock = new LinkedList<Tuple>();
	}
	
	/*
	 * Constructor for BNLJ
	 * This version can be used if you know name of the file of the outer loop
	 * and also number of attributes in the left file
	 */
	public PhyJoinBNLJOp(int bufferPages, int attrCounts, String lfile) {
		this(bufferPages);
		setLeftFile(attrCounts, lfile);
	}
	
	/**
	 * set left file name and also init tupleReader for the left file
	 * @param lname name of the left file
	 * 
	 */
	public void setLeftFile(int attrCount, String lname) {
		attrCounts = attrCount;
		computeNumberOfTuplesPerBlock();
	}
	
	/**
	 * compute number of tuples that can be put in a block
	 */
	public void computeNumberOfTuplesPerBlock() {
		tuplesPerBlock = (int)Math.floor((double)bufferSizeInPages * 4096 / (4 * attrCounts));
	}
	
	/**
	 * reset block by clearing the block and setting its inner index to 0
	 */
	public void resetBlock() {
		innerIndex = 0;
		tupleBlock.clear();
	}
	
	/**
	 * first reset tuple block and then fill tupleBlock until either block is full or no more tuples to read from
	 */
	public void fillTupleBlock() {
		resetBlock();
		Tuple temp;
		
		try {
			while ((tupleBlock.size() < tuplesPerBlock) && ((temp = child.getNextTuple()) != null) ) {
				tupleBlock.add(temp);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		if (!blockStarted) {		//check whether first time using tupleBlock
			blockStarted = true;  
			fillTupleBlock();
			rightTuple = rChild.getNextTuple();
			
		}
		while (tupleBlock.size() > 0) {
			while (rightTuple != null) {
				while (innerIndex < tupleBlock.size()) {
					Tuple join = new Tuple();
					Tuple leftTuple = tupleBlock.get(innerIndex++);
					for (int i: leftTuple.data) {
						join.data.add(i);
					}
					for (int k: rightTuple.data) {
						join.data.add(k);
					}
					
					for(int i = 0; i < conditions.size(); i++) {
						Condition c = conditions.get(i);
						if(!c.test(join, schema)) {
							break;
						}
						
						if(i == conditions.size() - 1) {
							return join;
						}
					}
				}
				innerIndex = 0;
				rightTuple = rChild.getNextTuple();
			}
			fillTupleBlock();
			if (tupleBlock.size() == 0) return null;
			rChild.reset();
			rightTuple = rChild.getNextTuple();
		}
		return null;
	}

	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		blockStarted = false;	//reset some of the bookkeeping variables and reopen tupleReader
		resetBlock();
		
		child.reset();
		rChild.reset();
	}

}
