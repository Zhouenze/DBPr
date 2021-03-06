package base;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map.Entry;

import static java.nio.file.StandardCopyOption.*;

import logicalPlan.LogPlan;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import physicalPlan.PhyPlan;
import physicalPlan.PhyScanBfOp;
import physicalPlan.PhySortExOp;

/*
 * DBPrPro2Main
 * Entrance of the whole project.
 * 
 * @author Enze Zhou ez242
 */
public final class DBPrPro2Main {
	
	/**
	 * Function that delete everything referred by path.
	 * 
	 * @param
	 * 		path: the file or directory that is going to be deleted.
	 */
	private static void delAll(String path) {
    	 File f = new File(path);
    	 if (!f.exists())
    		 return;
    	 if (f.isDirectory()) {
    		 String [] list = f.list();
    		 for (int i = 0; i < list.length; ++i)
    			 delAll(path + "/" + list[i]);
    	 }
    	 f.delete();
	}
	
	/*
	 * Function that clears the temp file, which is called after every query.
	 */
	private static void clearFolder(String path) {
		File fil = (path == null ? new File(DBCatalog.getCatalog().tempPath) : new File(path));
		String [] list = fil.list();
		for (int i = 0; i < list.length; ++i)
			delAll(DBCatalog.getCatalog().tempPath + list[i]);
	}


	/**
	 * main method of this project.
	 * @param args[0]
	 * 		input folder path without / at the end.
	 */
	public static void main(String[] args) {
		
		// Build DBCatalog first.
		try {
			DBCatalog.getCatalog().setSchema(args[0]);
		} catch (IOException e) {
			System.err.println("IOException occurred when building catalog: " + e.toString());
		}
		
		// Output the catalog built.
		System.out.println("Built catalog:");
		DBCatalog.getCatalog().print();
		System.out.println();
		
			
		// Build indexes. Clear previous indexes
		String append = (DBCatalog.getCatalog().inputPath.contains("/") ? "/db/indexes/" : "\\db\\indexes\\");
		clearFolder(DBCatalog.getCatalog().inputPath + append);
		
		try {
			for (Entry<String, DBCatalog.RelationInfo> relation : DBCatalog.getCatalog().tables.entrySet()) {
				for (DBCatalog.IndexInfo indexInfo : relation.getValue().indexes) {
					if (indexInfo.clustered == 1) {
						PhyScanBfOp tempScan = new PhyScanBfOp();
						tempScan.fileName = relation.getKey();
						tempScan.alias = relation.getKey();
						PhySortExOp tempSort = new PhySortExOp(10);
						tempSort.child = tempScan;
						tempSort.buildSchema();
						tempSort.sortAttrs.add(relation.getKey() + "." + indexInfo.keyName);
						append = (DBCatalog.getCatalog().inputPath.contains("/") ? "db/data/": "db\\data\\");
						
						// Output human readable for debugging. Can be omitted.
						tempSort.dumpReadable(new FileOutputStream(DBCatalog.getCatalog().inputPath + append + relation.getKey() + "_humanreadable"));
						Files.move(Paths.get(tempSort.getResultPath()), Paths.get(DBCatalog.getCatalog().inputPath + append + relation.getKey()), REPLACE_EXISTING);
					}
					
					// Build index
					new BTreeIndex(relation.getKey(), indexInfo.keyName, indexInfo.clustered == 1, indexInfo.order);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		// Evaluate queries if needed.
		try {
			// Initialize a SqlParser.
			CCJSqlParser sqlParser = new CCJSqlParser(new FileReader(DBCatalog.getCatalog().inputPath + "queries.sql"));
			Statement statement;
			
			// For every select statement.
			int i = 1;
			while ((statement = sqlParser.Statement()) != null) {
				try {
					// Build logical plan.
					LogPlan plan = new LogPlan((Select) statement);
					System.out.println(plan.query);
//					plan.print(null);
//					System.out.println();
					plan.print(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + (i++) + "_logicalplan"));
					
					// Build physical plan and run it.
					PhyPlan phyPlan = new PhyPlan(plan);
//					phyPlan.print(null);
//					System.out.println();
					phyPlan.print(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + (i-1) + "_physicalplan"));
					
					long startTime = System.currentTimeMillis();
					phyPlan.root.dump(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + (i-1)));
					long endTime = System.currentTimeMillis();
					long runtime = endTime - startTime;
					System.out.println("Run time of query " + (i-1) + ": " + runtime + "\n");
					
					// Output human readable for debugging. Can be omitted.
					phyPlan.root.reset();
					phyPlan.root.dumpReadable(new FileOutputStream(DBCatalog.getCatalog().outputPath + "query" + (i-1) + "_humanreadable"));
					
					clearFolder(null);

				// Catch every exception so that the program can go on to next statement.
				} catch (Exception e) {
					System.err.println("Exception occurred for statement: " + statement);
					System.err.println(e.toString());
					e.printStackTrace();
				}
			}
			
		} catch (Exception e) {
			System.err.println("Exception occurred with CCJSqlParser: " + e.toString());
		}
 
	}
}
