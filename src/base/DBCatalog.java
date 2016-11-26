package base;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/*
 * DBCatalog
 * Catalog of this database, including input/output path and all the schema information of its tables loaded from schema.txt.
 * Singleton pattern because only one catalog instance is needed for every database.
 * 
 * @author Enze Zhou ez242
 */
public final class DBCatalog {
	
	public class IndexInfo {
		public String keyName;
		public Integer clustered;
		
		public IndexInfo(String keyName, String clustered) {
			this.keyName = keyName;
			this.clustered = Integer.valueOf(clustered);
		}
	}
	
	public class AttrInfo {
		public String name;
		public Integer highValue;
		public Integer lowValue;
		
		public AttrInfo(String attrInfo) {
			String [] attrInfoSplit = attrInfo.split(",");
			name = attrInfoSplit[0];
			lowValue = Integer.valueOf(attrInfoSplit[1]);
			highValue = Integer.valueOf(attrInfoSplit[2]);
		}
	}
	
	public class RelationInfo {
		public Integer tupleNum;
		public Vector<AttrInfo> attrs = new Vector<>();
		public Vector<IndexInfo> indexes = new Vector<>();
		
		public int findIdOfAttr(String attrName) {
			for (int i = 0; i < attrs.size(); ++i)
				if (attrName.equals(attrs.get(i).name))
					return i;
			return -1;
		}
		
		public IndexInfo findIndexOfKey(String keyName) {
			for (IndexInfo index : indexes)
				if (index.keyName.equals(keyName))
					return index;
			return null;
		}
	}
	
	public HashMap<String, RelationInfo> tables = null;
	
	
	
	
	
	
	private static DBCatalog catalog = new DBCatalog();		// The only instance of this class.
	
	public int pageSize = 4096;								// page size of files.
	public String inputPath = "";							// inputPath from cmd with a / at the end.
	public String outputPath = "";							// outputPath from cmd with a / at the end.
	public String tempPath = "";							// tempPath from cmd with a / at the end.
//	public HashMap<String, Vector<String>> tables = null;	// Schema information of all the tables saved as a dictionary.
															// Key is the table name and value is a vector of all the column names of that table.
//	public HashMap<String, String> indexKeys = null;		// Map names of relations who has an index to the key column name of these indexes.
//	public HashMap<String, Integer> indexClustered = null;	// Map names of relations who has an index to whether the index is clustered.
	
//	public boolean buildIndexes = false;					// Whether build index is required.
//	public boolean evaluateQueries = false;					// Whether evaluate queries is required.
	
	/*
	 * Constructor is private for singleton pattern.
	 */
	private DBCatalog() {}
	
	/*
	 * Get the only instance of this class.
	 * @return the only instance of this class.
	 */
	public static DBCatalog getCatalog() { return catalog; }
	
	/*
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
//		configLine = configReader.readLine();
//		buildIndexes = (Integer.valueOf(configLine.trim()) == 1);
//		configLine = configReader.readLine();
//		evaluateQueries = (Integer.valueOf(configLine.trim()) == 1);
		configReader.close();
		
//		tables = new HashMap<>();
//		
//		// Read from schema.txt to build schema information.
//		String append = (this.inputPath.contains("/") ? "db/schema.txt" : "db\\schema.txt");
//		BufferedReader schemaReader = new BufferedReader(new FileReader(this.inputPath + append));
//		String schemaLine = null;
//		while ((schemaLine = schemaReader.readLine()) != null) {
//			String [] columns = schemaLine.trim().split(" ");
//			Vector<String> columnsVec = new Vector<>();
//			for (int i = 1; i < columns.length; ++i) {
//				columnsVec.add(columns[i]);
//			}
//			tables.put(columns[0], columnsVec);
//		}
//		schemaReader.close();
		
//		// Read from index info file to get index setting.
//		indexKeys = new HashMap<>();
//		indexClustered = new HashMap<>();
//		String append = (this.inputPath.contains("/") ? "db/index_info.txt" : "db\\index_info.txt");
//		BufferedReader indexesReader = new BufferedReader(new FileReader(this.inputPath + append));
//		String indexLine = null;
//		while ((indexLine = indexesReader.readLine()) != null) {
//			String [] indexConfig = indexLine.trim().split(" ");
//			indexKeys.put(indexConfig[0], indexConfig[1]);
//			indexClustered.put(indexConfig[0], Integer.valueOf(indexConfig[2]));
//		}
//		indexesReader.close();
		
		
		
		
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
			tables.get(indexConfig[0]).indexes.add(new IndexInfo(indexConfig[1], indexConfig[2]));
		}
		indexesReader.close();
		
		
		
		
		
		
		
		
		
		
		
	}
	
	/*
	 * Print catalog information.
	 */
	public void print() {
		System.out.println("inputPath:\t" + inputPath);
		System.out.println("outputPath:\t" + outputPath);
		System.out.println("tempPath:\t" + tempPath);
//		System.out.println("buildIndexes:\t" + buildIndexes);
//		System.out.println("evaluateQueries:\t" + evaluateQueries);
		
//		System.out.println("Table schemas:");
//		Set<String> tableNames = tables.keySet();
//		for (String tableName : tableNames) {
//			System.out.print("\t" + tableName + ":\t");
//			Vector<String> columnNames = tables.get(tableName);
//			for (int j = 0; j < columnNames.size(); ++j) {
//				System.out.print(columnNames.get(j) + " ");
//			}
//			System.out.println();
//		}
//		
//		System.out.println("Index schemas:");
//		Set<String> indexFileNames = indexKeys.keySet();
//		for (String indexFileName : indexFileNames) {
//			System.out.println("\t" + indexFileName + ":\t" + indexKeys.get(indexFileName) + "\t" + indexClustered.get(indexFileName));
//		}
		
		System.out.println("Relation info:");
		Set<String> tableNames = tables.keySet();
		for (String tableName : tableNames) {
			System.out.println("\t" + tableName + ":");
			RelationInfo table = tables.get(tableName);
			System.out.print("\t\t" + table.tupleNum + "  ");
			for (int j = 0; j < table.attrs.size(); ++j) {
				System.out.print(table.attrs.get(j).name + ',' + table.attrs.get(j).lowValue + ',' + table.attrs.get(j).highValue + "  ");
			}
			System.out.println();
			
			System.out.print("\t\t");
			for (int j = 0; j < table.indexes.size(); ++j) {
				System.out.print(table.indexes.get(j).keyName + ',' + table.indexes.get(j).clustered + "  ");
			}
			System.out.println();
		}
	}
}
