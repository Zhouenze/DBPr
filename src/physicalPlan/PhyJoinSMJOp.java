package physicalPlan;

import java.io.File;
import java.io.IOException;

import base.Condition;
import base.DBCatalog;
import base.Tuple;
import base.TupleReader;
import base.TupleWriter;

public class PhyJoinSMJOp extends PhyJoinOp{
	
	static int count = 0;
	
	String filePath = "";
	TupleReader TR = null;
	Tuple outer = null;
	Tuple inner = null;
	Tuple savedInner = null;
	boolean over = false;
	boolean init = true;
	
	public PhyJoinSMJOp() {
		super();
		filePath = DBCatalog.getCatalog().tempPath + "SMJtemp" + count;
		++count;
	}
	

	@Override
	public Tuple getNextTuple() {
		if (over)
			return null;
		
		if (init) {
			init = false;
			outer = child.getNextTuple();
			inner = rChild.getNextTuple();
			
		} else {	// TR is valid except when initialization
					// If next valid tuple is in the same group, it can be detected here
			try {
				inner = TR.getNextTuple();
			} catch(Exception e) {
				System.err.println("Exception occurred for reading in SMJ.");
				System.err.println(e.toString());
				e.printStackTrace();
			}
			if (inner == null) {
				outer = child.getNextTuple();
				if (outer == null) {
					over = true;
					return null;
				}
				try {
					TR.reset();
					inner = TR.getNextTuple();
				} catch (IOException e) {
					System.err.println("Exception occurred for reading in SMJ.");
					System.err.println(e.toString());
					e.printStackTrace();
				}
			}
			
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
			
			if (savedInner == null) {
				over = true;
				return null;
			}
			inner = savedInner;
		}
		
		// The last group has passed, find next valid group here.
		while (true) {
			int cmp = compareOI(outer, inner);
			if (cmp == 0) {
				refreshFile();			//jump the first one!
				Tuple join = new Tuple();
				for(int i: outer.data)
					join.data.add(i);
				for(int j: inner.data)
					join.data.add(j);
				return join;
			} else if (cmp < 0) {
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
	
	void refreshFile() {
		File file = new File(filePath);
		if (file.exists())
			file.delete();
		try {
			TupleWriter TW = new TupleWriter(filePath);
			TW.setNextTuple(inner);
			Tuple temp = rChild.getNextTuple();
			while (temp != null && compareII(temp, inner) == 0) {
				TW.setNextTuple(temp);
				temp = rChild.getNextTuple();
			}
			savedInner = temp;
			if (!TW.bufferEmpty())
				TW.fillFlush();
			TW.close();
			if (TR != null)
				TR.close();
			TR = new TupleReader(filePath);
			TR.getNextTuple();
		} catch (IOException e) {
			System.err.println("Exception occurred for reading in SMJ.");
			System.err.println(e.toString());
			e.printStackTrace();
		}
	}

	@Override
	public void reset() {
		child.reset();
		rChild.reset();
		over = false;
		init = true;
		outer = null;
		inner = null;
		savedInner = null;
		
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
	
	private int compareOI(Tuple outer, Tuple inner) {
		for (Condition cond : conditions) {
			int ou = outer.data.get(child.schema.get(cond.leftName));
			int in = inner.data.get(rChild.schema.get(cond.rightName));
			if (ou != in)
				return ou < in ? -1 : 1;
		}
		return 0;
	}
	
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
