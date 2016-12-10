package base;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * DBCatalog
 * Catalog of this database, including input/output path and all the schema information of its tables loaded from schema.txt.
 * Singleton pattern because only one catalog instance is needed for every database.
 * 
 * @authors Enze Zhou ez242, Weicheng Yu wy248
 */
public final class DBCatalog {
	
	/*
	 * Info class for an index, contains part name of key and if it's clustered or not.
	 * Objects will belong to a relation so no filename needed.
	 */
	public class IndexInfo {
		public String keyName;			// Part name.
		public Integer clustered;
		public Integer leafNum;			// Not set when construction. Will be set when used and later can accelerate process.
		public Integer order;
		
		/**
		 * Constructor of index info class.
		 * @param
		 * 		keyName: name of key.
		 * 		clustered: if the index is clustered.
		 */
		public IndexInfo(String keyName, String clustered, String order) {
			this.keyName = keyName;
			this.clustered = Integer.valueOf(clustered);
			this.order = Integer.valueOf(order);
			leafNum = -1;
		}
	}
	
	/*
	 * Info class for attributes.
	 * Objects will belong to a relation so no filename needed.
	 */
	public class AttrInfo {
		public String name;				// Part name.
		public Integer highValue;		// Largest value according to statistics.
		public Integer lowValue;		// Smallest value according to statistics.
		
		/**
		 * Constructor.
		 * @param
		 * 		attrInfo: string containing information of this attribute, like "G,0,5000"
		 */
		public AttrInfo(String attrInfo) {
			String [] attrInfoSplit = attrInfo.split(",");
			name = attrInfoSplit[0];
			lowValue = Integer.valueOf(attrInfoSplit[1]);
			highValue = Integer.valueOf(attrInfoSplit[2]);
		}
	}
	
	/*
	 * Info class of a relation.
	 */
	public class RelationInfo {
		public Integer tupleNum;
		public Vector<AttrInfo> attrs = new Vector<>();
		public Vector<IndexInfo> indexes = new Vector<>();
		
		/**
		 * Function to get an attribute with particular part name.
		 * @param
		 * 		attrName: part name of the attribute.
		 */
		public AttrInfo findAttr(String attrName) {
			for (AttrInfo attrInfo : attrs)
				if (attrInfo.name.equals(attrName))
					return attrInfo;
			return null;
		}
		
		/**
		 * Get attribute index.
		 * @param
		 * 		attrName: part name of the attribute.
		 */
		public int findIdOfAttr(String attrName) {
			for (int i = 0; i < attrs.size(); ++i)
				if (attrs.get(i).name.equals(attrName))
					return i;
			return -1;
		}
		
		/**
		 * Get index with keyName
		 * @param
		 * 		keyName: part key name of the index.
		 */
		public IndexInfo findIndexOfKey(String keyName) {
			for (IndexInfo index : indexes)
				if (index.keyName.equals(keyName))
					return index;
			return null;
		}
	}
	
	public HashMap<String, RelationInfo> tables = null;		// Map file names to information of that relation.
	
	
	
	private static DBCatalog catalog = new DBCatalog();		// The only instance of this class.
	
	public int pageSize = 4096;								// page size of files.
	public String inputPath = "";							// inputPath from cmd with a / at the end.
	public String outputPath = "";							// outputPath from cmd with a / at the end.
	public String tempPath = "";							// tempPath from cmd with a / at the end.
	
	/*
	 * Constructor is private for singleton pattern.
	 */
	private DBCatalog() {}
	
	/**
	 * Get the only instance of this class.
	 * @return the only instance of this class.
	 */
	public static DBCatalog getCatalog() { return catalog; }
	
	/**
	 * Use inputPath and outputPath to build catalog information.
	 * @param inputPath
	 * 		input path from cmd line.
	 * @param outputPath
	 * 		output path from cmd line.
	 */
	public void setSchema(String configPath) throws IOException {
		
		// Read configuration file.
		BufferedReader configReader = new BufferedReader(new FileReader(configPath));
		String configLine = null;
		configLine = configReader.readLine();
		this.inputPath = configLine.trim() + (configLine.contains("/")?"/":"\\");
		configLine = configReader.readLine();
		this.outputPath = configLine.trim() + (configLine.contains("/")?"/":"\\");
		configLine = configReader.readLine();
		this.tempPath = configLine.trim() + (configLine.contains("/")?"/":"\\");
		configReader.close();
		
		// gather statistics
		gatherStats();
		
		tables = new HashMap<>();
		
		// Read from stat.txt to build schema information.
		String append = (this.inputPath.contains("/") ? "db/stat.txt" : "db\\stat.txt");
		BufferedReader statReader = new BufferedReader(new FileReader(this.inputPath + append));
		String statLine = null;
		while ((statLine = statReader.readLine()) != null) {
			String [] columns = statLine.trim().split(" ");
			
			RelationInfo relation = new RelationInfo();
			relation.tupleNum = Integer.valueOf(columns[1]);
			for (int i = 2; i < columns.length; ++i)
				relation.attrs.add(new AttrInfo(columns[i]));
			
			tables.put(columns[0], relation);
		}
		statReader.close();
		
		// Read from index info file to get index setting.
		append = (this.inputPath.contains("/") ? "db/index_info.txt" : "db\\index_info.txt");
		BufferedReader indexesReader = new BufferedReader(new FileReader(this.inputPath + append));
		String indexLine = null;
		while ((indexLine = indexesReader.readLine()) != null) {
			String [] indexConfig = indexLine.trim().split(" ");
			for (int i = 1; i < indexConfig.length; i += 3)
				tables.get(indexConfig[0]).indexes.add(new IndexInfo(indexConfig[i], indexConfig[i + 1], indexConfig[i + 2]));
		}
		indexesReader.close();
	}
	
	/*
	 * Gather statistics about data and write stats to db/stat.txt
	 */
	public void gatherStats() {
		try {
			BufferedReader schemaReader = new BufferedReader(new FileReader(this.inputPath+"db/schema.txt"));
			File statFile = new File(this.inputPath+"db/stat.txt");
			statFile.createNewFile();
            BufferedWriter statFileWriter = new BufferedWriter(new FileWriter(statFile));
            
			String line = null;
			Tuple tempTuple;
			while ((line=schemaReader.readLine()) != null) {
				
				String [] content = line.trim().split(" ");
				TupleReader tempTR = new TupleReader(this.inputPath+"db/data/"+content[0]);
				Map<String, ArrayList<Integer>> tempInfo = new HashMap<String, ArrayList<Integer>>();
				for (int i = 1; i < content.length; ++i) {
					tempInfo.put(content[i], new ArrayList<Integer>());
					tempInfo.get(content[i]).add(Integer.MAX_VALUE);
					tempInfo.get(content[i]).add(Integer.MIN_VALUE);
				}
				int tempCount = 0;
				while ((tempTuple = tempTR.getNextTuple()) != null) {
					++tempCount;
					int tempInt = 0 ;
					for (int j = 0; j < tempTuple.data.size(); ++j) {
						ArrayList<Integer> tempArr = tempInfo.get(content[j+1]);
						tempInt = tempTuple.data.get(j);
						if (tempInt < tempArr.get(0)) {
							tempArr.set(0, tempInt);
						}
						if (tempInt > tempArr.get(1)) {
							tempArr.set(1,  tempInt);
						}
					}
				}
				String minMaxStr = "";
				for (int k = 1; k < content.length; ++k) {
					ArrayList<Integer> tempArr2 = tempInfo.get(content[k]);
					minMaxStr += content[k]+","+String.valueOf(tempArr2.get(0)) + "," + String.valueOf(tempArr2.get(1)) + " ";
					
				}
	
//				System.out.println(minMaxStr);
				statFileWriter.write(content[0] + " " + String.valueOf(tempCount) + " " + minMaxStr + "\n" );
				
			}
			schemaReader.close();
			statFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/*
	 * Print catalog information.
	 */
	public void print() {
		System.out.println("inputPath:\t" + inputPath);
		System.out.println("outputPath:\t" + outputPath);
		System.out.println("tempPath:\t" + tempPath);
		
		System.out.println("Relation info:");
		Set<String> tableNames = tables.keySet();
		for (String tableName : tableNames) {
			System.out.println("\t" + tableName + ":");
			RelationInfo table = tables.get(tableName);
			
			// print schema information and statistics
			System.out.print("\t\t" + table.tupleNum + "  ");
			for (int j = 0; j < table.attrs.size(); ++j) {
				System.out.print(table.attrs.get(j).name + ',' + table.attrs.get(j).lowValue + ',' + table.attrs.get(j).highValue + "  ");
			}
			System.out.println();
			
			// print index information
			System.out.print("\t\t");
			for (int j = 0; j < table.indexes.size(); ++j) {
				System.out.print(table.indexes.get(j).keyName + ',' + table.indexes.get(j).clustered + "  ");
			}
			System.out.println();
		}
	}
}
