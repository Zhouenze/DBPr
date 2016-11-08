package base;

/*
 * A level of abstration to help with reading records stored in files
 * 
 * @author Weicheng Yu wy248
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class TupleReader {
	private String fileName;
	private FileChannel FC;
	private ByteBuffer BB;
	private int tupleIndex;
	private int tupleSize;
	private int tupleCounts;
	private int tupleCurrentCounts;
	private int tupleCurrentPages = -1;
	
	@SuppressWarnings("resource")
	public TupleReader(String filename) throws IOException {
		fileName = filename;
		FC = new FileInputStream(filename).getChannel();
		BB = ByteBuffer.allocate(4096); //hardcoded page size
		readTupleInfo();
		
		tupleCurrentPages = 0;
	}
	
	/*
	 * Method that is used to read meta data
	 */
	private int readTupleInfo() throws IOException {
		//read tuple meta data
		int ret = FC.read(BB);
		BB.flip();
		if (ret > 0){
			tupleSize = BB.getInt(0);
			tupleCounts = BB.getInt(4);
			tupleIndex = 8;
			tupleCurrentCounts = 0;
			++tupleCurrentPages;
		}
		return ret;
	}
	
	/*
	 * Jump some pages. This function is only called by index scan operator 
	 * when it needs to jump to the first valid tuple in a clustered relation.
	 * @param
	 * 		pageCount: 	number of pages to jump. If we want to go to page ID 6, we jump 6 pages to go there because this function is
	 * 					called immediately after initialization where one page has already been read..
	 */
	public void jumpPages(int pageCount) {
		try {
			int ret = 0;
			for (int i = 0; i < pageCount; ++i) {
				ret = FC.read(BB);
				BB.flip();
			}
			tupleCurrentPages += pageCount;
			if (ret > 0){
				tupleSize = BB.getInt(0);
				tupleCounts = BB.getInt(4);
				tupleIndex = 8;
				tupleCurrentCounts = 0;	
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Return the next key-rid pair in form of tuple.
	 * @param
	 * 		keyId: the index of key of this tuple.
	 * @return
	 * 		a tuple of three parts: key of this next tuple, page index and tuple index on this page
	 */
	public Tuple getNextKeyRid(int keyId) throws IOException{

		// If a page has no tuple, return null directly 
		if (tupleCounts == 0) return null;

		Tuple ret = new Tuple();
		if (tupleCurrentCounts < tupleCounts) {
			ret.data.add(BB.getInt(tupleIndex + 4 * keyId));
			tupleIndex += (tupleSize * 4);
		} else {
			BB.clear();
			if (readTupleInfo() <= 0){
				return null;
			}
			
			ret.data.add(BB.getInt(tupleIndex + 4 * keyId));
			tupleIndex += (tupleSize * 4);
		}
		
		ret.data.add(tupleCurrentPages);
		ret.data.add(tupleCurrentCounts);
		
		++tupleCurrentCounts;
		return ret;
	}
	
	
	/*
	 * Method that is used to grab a record
	 */
	public Tuple getNextTuple() throws IOException{

		//if a page has no tuple, return null directly 
		//previously calling getNextTuple() will return a tuple object with no data set. Now it returns null
		if (tupleCounts == 0) return null;


		Tuple ret = new Tuple();
		if (tupleCurrentCounts < tupleCounts) {
			for (int i = 0; i < tupleSize; ++i) {
				ret.data.add(BB.getInt(tupleIndex));
				tupleIndex += 4;
			}
		}
		else{
			BB.clear();
			if (readTupleInfo() <= 0){
				return null;
			}
			for (int i = 0; i < tupleSize; ++i) {
				ret.data.add(BB.getInt(tupleIndex));
				tupleIndex += 4;
			}		
		}
		++tupleCurrentCounts;
		return ret;

	}
	
	/*
	 * Method that is used to close an open file
	 */
	public void close() throws IOException{
		FC.close();
	}
	
	
	/*
	 * Method that is used to reset tuple reader
	 */
	@SuppressWarnings("resource")
	public void reset() throws IOException {
		FC.close();
		FC = new FileInputStream(fileName).getChannel();
		BB.clear();
		readTupleInfo();
		
		tupleCurrentPages = 0;
	}
	
	
	
	
	
	
	
	
}
