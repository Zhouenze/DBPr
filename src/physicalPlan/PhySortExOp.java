package physicalPlan;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Vector;

import base.DBCatalog;
import base.Tuple;
import base.TupleReader;
import base.TupleWriter;

/**
 * External Sort
 * This is the operator for external sort, which performs the sort
 * by first using B buffer pages to internally sort all the tuples from child into several partial sorted runs,
 * and then merging those runs into bigger sorted runs pass by pass,
 * until all merge into one whole result table.
 * 
 * @author Shuang Zhang sz468
 *
 */

public final class PhySortExOp extends PhySortOp {
	
	private static int count = 0;                                        // Number of External Sort Operators in current query
	private String tempsubdir = DBCatalog.getCatalog().tempPath;         // Path of the sub-directory of this operator
	// DBCatalog.getCatalog().tempPath; 
	private TupleReader TR = null;                                       // The Tuple Reader for reading and returning sort result
	private int B;                                                       // Number of Buffer Pages to be used in the sort
	private PriorityQueue<Tuple> internal = null;                        // Buffer to store B page of child tuples
	private boolean isSorted = false;                                    // flag of whether the sort has been performed or not
//	public boolean binary = true;                                       // flag of whether binary or human-readable format is used for scratch files (for debugging)
	public Vector<Integer> sortAttrsIndex = null;                       // Index of the attributes the tuples from child operator will be sorted on
	
	
	/*
	 * Constructor of External Sort with given # of buffer pages 
	 * and with associated sub-directory built accordingly to store "scratch" files
	 */
	public PhySortExOp(int BufferPages) {
		super();
		B = BufferPages;
		count++;
		tempsubdir += "sub" + count + "/";
		Path path = Paths.get(tempsubdir);
		try {
		    Files.createDirectories(path);
		} catch (IOException e) {
		    System.err.println("Cannot create directories - " + e);
		}
	}
	
	/*
	 * Get the path to result for this sort operator. This is used when building clustered index and replacing the original data.
	 * Just use the sort result file to replace the original one.
	 * @return
	 * 		Path to result file.
	 */
	public String getResultPath() {
		String append = (DBCatalog.getCatalog().inputPath.contains("/") ? "/sortResult" : "\\sortResult");
		return tempsubdir + append;
	}
	
	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		if(!isSorted) {
			try{
				externalSort();
			} catch(Exception e) {
				System.err.println("Exception occurred for sorting: " + tempsubdir);
				System.err.println(e.toString());
				e.printStackTrace();
			}
			isSorted = true;
		}
		
		// open a TupleReader on the temporary file, i.e. the Full Sorted File
		try{
			if(TR == null) {
				TR = new TupleReader(tempsubdir + "/sortResult");
			}
			Tuple next = TR.getNextTuple();
			if(next == null) {
				return null;
			}
//			System.out.println(next.data);
			return next;
		} catch(Exception e) {
			System.err.println("Exception occurred for reading in external sort result for: " + tempsubdir);
			System.err.println(e.toString());
			e.printStackTrace();
		}
		
		return null;
	}
	
	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() { // ? will it ever be reset before sorting?
		if(TR == null) {
			return;
		}
		try{
			TR.reset();
		} catch(Exception e) {
			System.err.println("Exception occurred for resetting the TupleReader on: " + tempsubdir);
			System.err.println(e.toString());
			e.printStackTrace();
		}
	}
	
	/*
	 * Method to perform external sort and write out the Full Sorted File
	 */
	private void externalSort() throws IOException{
		/*
		 *  Pass 0: internal sort
		 */
		// Read in from child；Write out runs of B pages;
		
		int numPerRun = B * 4096 / (schema.size() * 4); // # of tuples per run given in Pass0
		boolean buildMore;
		int numRuns = 0;
		if(sortAttrsIndex == null) {
			this.buildAttrsIndex();
		}
		exComparator myComp = new exComparator();
		internal = new PriorityQueue<Tuple>(myComp);
		do {
			buildMore = this.buildHeap(numPerRun);
			numRuns++;
			
			// write out the (numRuns)th run
			TupleWriter TW;
			if(!buildMore && numRuns == 1) {
				TW = new TupleWriter(new FileOutputStream(tempsubdir + "sortResult"));
			} else {
				TW = new TupleWriter(new FileOutputStream(tempsubdir + "0_" + numRuns));
			}
			while(!internal.isEmpty()) {
//				System.out.println(internal.peek().data);
				TW.setNextTuple(internal.poll()); 
			}
			// leftover, fill with zero and write out
			if(!TW.bufferEmpty()) {       // TW would never know the end of writing
				TW.fillFlush();           
			}
			TW.close();
			// internal must be empty until this point
		}while (buildMore);
		
		/*
		 *  Pass 1 and any following
		 */
		if(numRuns > 1) { // if numRuns generated in Pass 0 is 1 already, file sorted already, no need to merge
			this.merge(numRuns, myComp);
		}
		
	}
	
	/*
	 * Method to perform the merge phase of external sort
	 */
	private void merge(int numRuns, exComparator myComp) throws IOException {
		// Pass 1 and any following
		// Read in the partial sorted runs from last pass; Merge; Write out;
		TupleReader[] mergeReader = new TupleReader[B - 1];
		int pass = 1;
		int numRunsToBuild = 0;
		PriorityQueue<Tuple> tempMerg = new PriorityQueue<Tuple>(myComp);
		Integer marker = null;
		while(true) {
			// last pass:  pass-1 , # runs: numRuns;
			// this pass:   pass  , # runs: numRunsToBuild;
			numRunsToBuild = numRuns % (B - 1) == 0 ? numRuns / (B - 1) : (numRuns / (B - 1) + 1);
			Map<Tuple, Integer> findReader;
			for(int i = 0; i < numRunsToBuild; i++) {
				findReader = new HashMap<>();
				for(int j = 0; j < B - 1; j++) {
					int runIndex = i * (B - 1) + j + 1; // index of the run to be read in last pass, which is (pass - 1)
					if(runIndex > numRuns) {
						marker = j;
						break;
					}
					mergeReader[j] = new TupleReader(tempsubdir + String.valueOf(pass-1) + "_" + runIndex);
				}
				
//				System.out.println("Pass" + pass + "," + i + "th Run");
				int numReader = marker == null ? B - 1 : marker;
				for(int k = 0; k < numReader; k++) {
					Tuple temp = mergeReader[k].getNextTuple();
					if(temp == null) {
						continue;
					}
					tempMerg.offer(temp);
					findReader.put(temp, k);
				}
				
				TupleWriter TW;
				if(numRunsToBuild == 1) {
					TW = new TupleWriter(new FileOutputStream(tempsubdir + "sortResult"));
				} else {
					TW = new TupleWriter(new FileOutputStream(tempsubdir + pass + "_" + String.valueOf(i + 1)));
				}
				while(!tempMerg.isEmpty()) {
					Tuple temp = tempMerg.poll();
					TW.setNextTuple(temp);
					Tuple next = mergeReader[findReader.get(temp)].getNextTuple();
					if(next != null) {
						tempMerg.offer(next);
						findReader.put(next, findReader.get(temp));
					}
				}
				if(!TW.bufferEmpty()) {
					TW.fillFlush();           
				}
				TW.close();
				for(int k = 0; k < numReader; k++) {
					mergeReader[k].close();
				}
			}
			
			if(numRunsToBuild == 1) {
				break;
			}
			numRuns = numRunsToBuild;
			pass++;
			marker = null;
		}
	}
	
	/*
	 * Method to update sortAttrsIndex based on sortAttrs
	 */
	private void buildAttrsIndex() {
		sortAttrsIndex = new Vector<Integer>();
		boolean[] inAttrs = new boolean[schema.size()];
		for(int i = 0; i < inAttrs.length; i++) {
			inAttrs[i] = false;
		}
		for(String attr: sortAttrs) {
			sortAttrsIndex.add(schema.get(attr));
			inAttrs[schema.get(attr)] = true;
		}
		for(int i = 0; i < inAttrs.length; i++) {
			if(!inAttrs[i]) {
				sortAttrsIndex.add(i);
			}
		}
	}
	
	/*
	 * Customized Comparator to compare tuples based on the sortAttrsIndex
	 */
	private class exComparator implements Comparator<Tuple> {
		public int compare(Tuple t1, Tuple t2) {
			if (sortAttrsIndex==null) {
				for (int j = 0; j < t1.data.size(); ++j) {
					int ret = t1.data.get(j).compareTo(t2.data.get(j));
					if (ret != 0) return ret;
				}
				return 0;
			}
			for(int index: sortAttrsIndex) {
				int result = t1.data.get(index).compareTo(t2.data.get(index));
				if(result != 0) {
					return result;
				}
			}
			return 0;
		}
		
	}
	
	/*
	 * Method to buffer B page's worth tuples to do internal sort in Pass 0
	 */
	private boolean buildHeap(int numPerRun){ 
		Tuple temp;
		int i = 0;
		while(i < numPerRun && (temp = child.getNextTuple()) != null) {
			internal.offer(temp);
			i++;
		}
//		System.out.println("internal heap has: " + i + "tuples");
		if(i != numPerRun || i == 0) { // i == 0, heap is empty!
			return false;
		}
		return true;
	}

}