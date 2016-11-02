package base;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class dataGenerator {
	
	public void generateData(String path, int seed) {
		
		try {
			TupleWriter TW = new TupleWriter(path);
			Random rand = new Random();
			rand.setSeed(seed);
			DataOutputStream dataOut = new DataOutputStream(new FileOutputStream(path + "readable"));
				
			for (int i = 0; i < 6000; ++i) {
				Tuple tp = new Tuple();
				for (int j = 0; j < 6; ++j) {
					Integer temp = rand.nextInt(1000);
					tp.data.add(temp);
					dataOut.writeBytes((j == 0 ? temp.toString() : "," + temp.toString()));
				}
				TW.setNextTuple(tp);
				dataOut.write('\n');
			}
			
			TW.close();
			dataOut.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
