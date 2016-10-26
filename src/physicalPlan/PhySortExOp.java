package physicalPlan;

import base.DBCatalog;
import base.Tuple;

/**
 * External Sort
 * 
 * 
 * @author Shuang Zhang sz468
 *
 */

public class PhySortExOp extends PhySortOp {
	
	public static int count = 0;      // Number of External Sort Operators in current query
	public String tempsubdir = "";    // Path of the sub-directory of this operator 防止count同时变了，contaminate了path
	// DBCatalog.getCatalog().tempPath; 
	public int B;                     // Number of Buffer Pages to be used in the sort
	public boolean isSorted = false;  // flag of whether the sort has been performed or not
	public boolean binary = true;     // flag of whether binary or human-readable format is used 
	                                  // for scratch files (for debugging)
	
	
	public PhySortExOp(int BufferPages) {
		B = BufferPages;
		count++;
		tempsubdir += "/sub" + count;  // ?这样会create新的下一级文件夹吗？
	}
	
	
	@Override
	public Tuple getNextTuple() {
		if(isSorted) {
			// open a TupleReader on the temporary file, i.e. the Full Sorted File
			
		} else { // call the sort and write out function
			externalSort();
			isSorted = true;
		}
		
		return null;
	}
	
	@Override
	public void reset() { // ? will it ever be reset before sorting?
		
	}
	
	/*
	 * Method to perform external sort and write out the Full Sorted File
	 */
	public void externalSort() {
		// read in the partial sorted runs from last pass
		// ? clean this sub-directory
		
		// for all the runs
		   // merge every (B-1) runs
		
		// write out to this sub-directory
		
	}
	
	// 我觉得我需要写个merge function as helper
	

}