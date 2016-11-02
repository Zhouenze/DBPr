package physicalPlan;

import java.io.File;
import java.io.IOException;

import base.Condition;
import base.DBCatalog;
import base.Tuple;
import base.TupleReader;
import base.TupleWriter;

/*
 * This class is the sort-merge-join operator.
 * 
 * @author Enze Zhou ez242
 */
public class PhyJoinSMJOp extends PhyJoinOp{
	
	String filePath = "";		// The size of the inner block that match the outer maybe very big, so I use a file to buffer it, 
								// because later we need to return to the beginning of it.
	TupleReader TR = null;		// Read from the file above.
	Tuple savedInner = null;	// When buffering inner block to file, the end is determined by a tuple that no longer match the outer.
								// It needs to be saved because if we call getNextTuple to get next one, this one will be lost.
	static int count = 0;		// Because tuples are buffered in a file, we need a way to distinguish different files.
	
	Tuple outer = null;
	Tuple inner = null;
	
	boolean over = false;		// Whether the output is over.
	boolean init = true;		// Whether it's the first getNextTuple operation. This is needed to do some initialization.
	
	/*
	 * Constructor of this operator
	 */
	public PhyJoinSMJOp() {
		super();
		filePath = DBCatalog.getCatalog().tempPath + "SMJtemp" + count;
		++count;
	}
	

	/*
	 * Method that returns next tuple in the output of this node.
	 * @override from super class Operator
	 * @return next tuple in the output of this node.
	 */
	@Override
	public Tuple getNextTuple() {
		
		if (over)
			return null;
		
		if (init) {
			init = false;
			outer = child.getNextTuple();
			inner = rChild.getNextTuple();
			
			if (inner == null || outer == null) {
				over = true;
				return null;
			}
			
		} else {	// TR is valid except when initialization.
					// If next valid tuple is in the same group (a group is a group of continuous outer tuples
					// that match a group of continuous inner tuples), it can be detected in this block.
			try {
				inner = TR.getNextTuple();
				if (inner == null) {
					outer = child.getNextTuple();
					if (outer == null) {
						over = true;
						return null;
					}
					TR.reset();
					inner = TR.getNextTuple();
				}
			} catch(Exception e) {
				System.err.println("Exception occurred for reading in SMJ.");
				System.err.println(e.toString());
				e.printStackTrace();
			}
			
			// Here both inner and outer will be available because every buffer file will have at least one tuple in it.
			if (compareOI(outer, inner) == 0) {
				Tuple join = new Tuple();
				for(int i: outer.data) {
					join.data.add(i);
				}
				for(int j: inner.data) {
					join.data.add(j);
				}
				return join;
			}
			
			// If the program get here, there is the current group is exhausted. We start looking for next match.
			if (savedInner == null) {
				over = true;
				return null;
			}
			inner = savedInner;
		}
		
		while (true) {
			int cmp = compareOI(outer, inner);
			
			if (cmp == 0) {						// If find a match.
				refreshFile();					// Refresh the buffer file for new group.
				Tuple join = new Tuple();
				for(int i: outer.data)
					join.data.add(i);
				for(int j: inner.data)
					join.data.add(j);
				return join;
				
			} else if (cmp > 0) {				// If not matching, go on to find next.
				inner = rChild.getNextTuple();
				if (inner == null) {
					over = true;
					return null;
				}
			} else {
				outer = child.getNextTuple();
				if (outer == null) {
					over = true;
					return null;
				}
			}
		}
	}
	
	/*
	 * This function is used to refresh the buffer file to get all the inner tuples in the matching group in it.
	 */
	void refreshFile() {

		try {
			// Close reader and delete invalid file.
			if (TR != null)
				TR.close();
			File file = new File(filePath);
			if (file.exists())
				file.delete();
			
			// Save all the tuples in the new matching group to file.
			TupleWriter TW = new TupleWriter(filePath);
			TW.setNextTuple(inner);
			Tuple temp = rChild.getNextTuple();
			while (temp != null && compareII(temp, inner) == 0) {
				TW.setNextTuple(temp);
				temp = rChild.getNextTuple();
			}
			
			// Copy temp for next search, close the file.
			savedInner = temp;
			if (!TW.bufferEmpty())
				TW.fillFlush();
			TW.close();
			
			// Open new filereader and jump the first one because it's already output
			TR = new TupleReader(filePath);
			TR.getNextTuple();
			
		} catch (IOException e) {
			System.err.println("Exception occurred for reading in SMJ.");
			System.err.println(e.toString());
			e.printStackTrace();
		}
	}

	/*
	 * Method that resets output of this node to the beginning.
	 * @override from super class Operator
	 */
	@Override
	public void reset() {
		child.reset();
		rChild.reset();
		over = false;
		init = true;
		
		outer = null;
		inner = null;
		savedInner = null;
		
		// Close file reader and delete file.
		if (TR != null)
			try {
				TR.close();
			} catch (IOException e) {
				System.err.println("Exception occurred for reading in SMJ.");
				System.err.println(e.toString());
				e.printStackTrace();
			}
		TR = null;
		
		File file = new File(filePath);
		if (file.exists())
			file.delete();
	}
	
	/*
	 *  Compare an inner tuple with an outer tuple to see if they.
	 *  @param
	 *  	inner1 and inner2 are two inner tuples being compared.
	 *  @return
	 *  	an integer determining which one is bigger.
	 */
	private int compareOI(Tuple outer, Tuple inner) {
		for (Condition cond : conditions) {
			int ou = outer.data.get(child.schema.get(cond.leftName));
			int in = inner.data.get(rChild.schema.get(cond.rightName));
			if (ou != in)
				return ou < in ? -1 : 1;
		}
		return 0;
	}
	
	/*
	 *  Compare two inner tuples to see if the matching group has ended.
	 *  @param
	 *  	inner1 and inner2 are two inner tuples being compared.
	 *  @return
	 *  	an integer determining which one is bigger
	 */
	private int compareII(Tuple inner1, Tuple inner2) {
		for (Condition cond : conditions) {
			int in1 = inner1.data.get(rChild.schema.get(cond.rightName));
			int in2 = inner2.data.get(rChild.schema.get(cond.rightName));
			if (in1 != in2)
				return in1 < in2 ? -1 : 1;
		}
		return 0;
	}

}