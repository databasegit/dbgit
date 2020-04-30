package ru.fusionsoft.dbgit.meta;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import com.diogonunes.jcdp.color.api.Ansi.FColor;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.data_table.ICellData;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.data_table.TreeMapRowData;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableData;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

/**
 * Meta class for Table data
 * @author mikle
 *
 */
public class MetaTableData extends MetaBase {
	 protected DBTable table = null;
	 private DBTableData dataTable = null;
	 
	 private TreeMapRowData mapRows = null;

	 public MetaTableData() {
		 setDbType();
		 setDbVersion();
	 }
	 
	 public MetaTableData(DBTable tbl) throws ExceptionDBGit {
		 setDbType();
		 setDbVersion();
		 setTable(tbl);
	 }
	 
	
	public DBTable getTable() {
		return table;
	}

	public TreeMap<String, RowData> getmapRows() {
		return mapRows;
	}
	
	public DBTableData getDataTable() {
		return dataTable;
	}
	
	public void setMapRows(TreeMapRowData mapRows) {
		this.mapRows = mapRows;
	}
	
	public void setDataTable(DBTableData dataTable) {
		this.dataTable = dataTable;
	}
	
	public void setTable(DBTable table) throws ExceptionDBGit {
		this.table = table;
		setName(table.getSchema()+"/"+table.getName()+"."+getType().getValue());
	}

	
	
	@Override
	public void setName(String name) throws ExceptionDBGit {
		if (table == null) {
			NameMeta nm = MetaObjectFactory.parseMetaName(name);
			table = new DBTable();
			table.setSchema(nm.getSchema());
			table.setName(nm.getName());
		}
		
		super.setName(name);
	}

	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitTableData;
	}

	public CSVFormat getCSVFormat() {
		return CSVFormat.DEFAULT
				//.withRecordSeparator("\n")
				.withDelimiter(';')
				.withNullString("<!NULL!>")
				.withQuote('"')
				//.withQuoteMode(QuoteMode.ALL)
				;	
	}
	
	public MetaTable getMetaTable() throws ExceptionDBGit {
		String metaTblName = table.getSchema()+"/"+table.getName()+"."+DBGitMetaType.DBGitTable.getValue();
		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
		
		IMapMetaObject dbObjs = gmdm.getCacheDBMetaData();
		MetaTable metaTable = (MetaTable) dbObjs.get(metaTblName);
		if (metaTable == null ) {
			metaTable = new MetaTable();
			metaTable.loadFromDB(table);
		}
		return metaTable;
	}
		
	public MetaTable getMetaTableFromFile() throws ExceptionDBGit {
		String metaTblName = table.getSchema()+"/"+table.getName()+"."+DBGitMetaType.DBGitTable.getValue();
		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
		
		MetaTable metaTable = (MetaTable)gmdm.loadMetaFile(metaTblName);		
		if (metaTable != null) 
			return metaTable;
		
		return getMetaTable();
	}
	
	
	@Override
	public boolean serialize(OutputStream stream) throws Exception {
		Integer count = 0;
		Set<String> fields = null;
		
		if (mapRows == null) {
			return false;
		}
		
		CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(stream), getCSVFormat());
		
		for (RowData rd : mapRows.values()) {
			if (count == 0) {
				fields = rd.getData().keySet();
				csvPrinter.printRecord(fields);					
			}
						
			rd.saveDataToCsv(csvPrinter, getTable());
			
			count++;
		}
		csvPrinter.close();
		return true;
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws Exception {
		
		MetaTable metaTable = getMetaTableFromFile();		
	
		CSVParser csvParser = new CSVParser(new InputStreamReader(stream), getCSVFormat());
		List<CSVRecord> csvRecords = csvParser.getRecords(); 
		
		if (csvRecords.size() > 0) {
			CSVRecord titleColumns = csvRecords.get(0);
				
			mapRows = new TreeMapRowData(); 
			
			for (int i = 1; i < csvRecords.size(); i++) {	
				RowData rd = new RowData(csvRecords.get(i), metaTable, titleColumns);
				mapRows.put(rd);			
			}
		}

		csvParser.close();
		
		//saveToFile("test");
		
		return this;
	}

	public boolean loadPortionFromDB(int currentPortionIndex) throws ExceptionDBGit {
		return loadPortionFromDB(currentPortionIndex, 0);
	}
	
	public boolean loadPortionFromDB(int currentPortionIndex, int tryNumber) throws ExceptionDBGit {
		try {
			IDBAdapter adapter = AdapterFactory.createAdapter();
			MetaTable metaTable = getMetaTable();
			if (metaTable.getFields().size() == 0)
				return false;
			
			dataTable = adapter.getTableDataPortion(table.getSchema(), table.getName(), currentPortionIndex, 0);
			
			ResultSet rs = dataTable.getResultSet();
			
			mapRows = new TreeMapRowData(); 
			
			while(rs.next()){
				RowData rd = new RowData(rs, metaTable);
				mapRows.put(rd);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			ConsoleWriter.println(e.getMessage());
			ConsoleWriter.println(e.getLocalizedMessage());
			
			try {
				if (tryNumber <= DBGitConfig.getInstance().getInteger("core", "TRY_COUNT", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000))) {
					try {
						TimeUnit.SECONDS.sleep(DBGitConfig.getInstance().getInteger("core", "TRY_DELAY", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000)));
					} catch (InterruptedException e1) {
						throw new ExceptionDBGitRunTime(e1.getMessage());
					}
					ConsoleWriter.println("Error while getting portion of data, try " + tryNumber);
					return loadPortionFromDB(currentPortionIndex, tryNumber++);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}			
			
			if (e instanceof ExceptionDBGit) 
				throw (ExceptionDBGit)e;
			throw new ExceptionDBGit(e);
		}
	}

	@Override
	public boolean loadFromDB() throws ExceptionDBGit {	
		try {			
			IDBAdapter adapter = AdapterFactory.createAdapter();
						
			MetaTable metaTable = getMetaTable();		
		
			if (metaTable.getFields().size() == 0)
				return false;
			
			List<String> idColumns = metaTable.getIdColumns();

			dataTable = adapter.getTableData(table.getSchema(), table.getName());
			
			if (dataTable.getErrorFlag() > 0) {
				ConsoleWriter.printlnColor(DBGitLang.getInstance().getValue("errors", "meta", "tooManyRecords").
						withParams(getName(), String.valueOf(IDBAdapter.MAX_ROW_COUNT_FETCH)), FColor.RED, 0);
				return false;
			}
			
			ResultSet rs = dataTable.getResultSet();
			
			mapRows = new TreeMapRowData(); 
			
			//System.out.println("load from db file "+getName());
			while(rs.next()){
				RowData rd = new RowData(rs, metaTable);
				mapRows.put(rd);
			}
			return true;
			/*
			System.out.println("******************************************");
			System.out.println();
			*/
		} catch (Exception e) {
			e.printStackTrace();
			if (e instanceof ExceptionDBGit) 
				throw (ExceptionDBGit)e;
			throw new ExceptionDBGit(e);
		}

	}
	
	public void diff(MetaTableData ob) throws Exception {
		if (mapRows.size() != ob.mapRows.size()) {
			System.out.println(DBGitLang.getInstance().getValue("general", "meta", "diffSize1").withParams(String.valueOf(mapRows.size()), String.valueOf(ob.mapRows.size())));
		}
		for (String rowHash : mapRows.keySet()) {
			RowData r1 = mapRows.get(rowHash);
			RowData r2 = ob.mapRows.get(rowHash);
			
			System.out.println(rowHash);
			System.out.println(r1.getData()+ " "+ r2.getData());
			
			if (r1.getData().size() != r2.getData().size()) {
				System.out.println(DBGitLang.getInstance().getValue("general", "meta", "diffSize2").withParams(rowHash));
			}
			
			for (String col : r1.getData().keySet()) {
				String d1 = r1.getData().get(col).convertToString();
				String d2 = r2.getData().get(col).convertToString();
				
				if (d1 != d2) {				
					if (!d1.equals(r2.getData().get(col))) {
						System.out.println(DBGitLang.getInstance().getValue("general", "meta", "diffDataRow").
								withParams(rowHash, col, r1.getData().get(col).toString(), r2.getData().get(col).toString()));
					}
				}
			}
		}
	}


	@Override
	public String getHash() {
		CalcHash ch = new CalcHash();
		if (mapRows == null) 
			return EMPTY_HASH;
		
		if (mapRows.size() == 0) 
			return EMPTY_HASH;
		
		//System.out.println(getName());
		int n = 0;
		for (RowData rd : mapRows.values()) {
			ch.addData(rd.getHashRow());
			//System.out.println("row "+n+" "+rd.getHashRow());
			n++;
		}
				
		return ch.calcHashStr();
	}
	
	@Override
	public int addToGit() throws ExceptionDBGit {		
		int count = super.addToGit(); 
				
		if (mapRows == null) return count;
		
		for (RowData rd : mapRows.values()) {
			for (ICellData cd : rd.getData().values()) {
				count += cd.addToGit();
			}			
		}
		
		return count;
	}
	
	@Override
	public int removeFromGit() throws ExceptionDBGit {
		int count = super.removeFromGit(); 
		
		if (mapRows == null)
			return 1;
		
		for (RowData rd : mapRows.values()) {
			for (ICellData cd : rd.getData().values()) {
				count += cd.removeFromGit();
			}			
		}
		
		return count;
	}

}
