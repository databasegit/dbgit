package ru.fusionsoft.dbgit.meta;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
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
import ru.fusionsoft.dbgit.data_table.MapFileData;
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
	 private List<String> fields = new ArrayList<>();

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

		//TODO ... which is not from file, but from db
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
				fields = rd.getData(this.fields).keySet();
				csvPrinter.printRecord(fields);
			}

			rd.saveDataToCsv(csvPrinter, getTable());

			count++;
		}
		csvPrinter.close();
		return true;
	}

	@Override
	public IMetaObject deSerialize(File file) throws Exception {
		MetaTable metaTable = getMetaTableFromFile();

		CsvReader csvReader = new CsvReader();
		csvReader.setFieldSeparator(';');
		int i = 1;

		try (CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8)) {
			CsvRow row;
			boolean flag = false;
			mapRows = new TreeMapRowData();
			CsvRow titleColumns = null;


			while ((row = csvParser.nextRow()) != null) {
				if (!flag) {
					titleColumns = row;
					fields = row.getFields();
				} else {
					RowData rd = new RowData(row, metaTable, titleColumns);
					mapRows.put(rd);
					i++;
				}
				flag = true;
			}
		} catch (Throwable ex){
			ConsoleWriter.detailsPrintlnRed(DBGitLang.getInstance().getValue("general", "meta", "loadRow").withParams(String.valueOf(i) ));
			warnFilesNotFound();
			throw ex;
		}
		ConsoleWriter.detailsPrintlnGreen(DBGitLang.getInstance().getValue("general", "meta", "loadedRow").withParams(String.valueOf(i) ));
		warnFilesNotFound();

		return this;
	}


	@Override
	public IMetaObject deSerialize(InputStream stream) throws Exception {

		MetaTable metaTable = getMetaTableFromFile();

		CSVParser csvParser = new CSVParser(new InputStreamReader(stream), getCSVFormat());
		List<CSVRecord> csvRecords = csvParser.getRecords();

		if (csvRecords.size() > 0) {
			CSVRecord titleColumns = csvRecords.get(0);
            fields.clear();
			for (int i = 0; i < csvRecords.get(0).size(); i++) {
			    fields.add(csvRecords.get(0).get(i));
            }

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

			if (dataTable.getErrorFlag() > 0) {
				ConsoleWriter.printlnColor(DBGitLang.getInstance().getValue("errors", "meta", "tooManyRecords").
						withParams(getName(), String.valueOf(IDBAdapter.MAX_ROW_COUNT_FETCH)), FColor.RED, 0);
				return false;
			}

			mapRows = new TreeMapRowData();

			boolean flag = false;
			while(rs.next()){

			    if (!flag) {
			        fields.clear();
                    for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                        String columnName = rs.getMetaData().getColumnName(i + 1);
                        if (columnName.equalsIgnoreCase("DBGIT_ROW_NUM"))
                            continue;
                        fields.add(columnName);
                    }
                }

			    flag = true;
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
			System.out.println(r1.getData(fields)+ " "+ r2.getData(ob.fields));
			
			if (r1.getData(fields).size() != r2.getData(ob.fields).size()) {
				System.out.println(DBGitLang.getInstance().getValue("general", "meta", "diffSize2").withParams(rowHash));
			}
			
			for (String col : r1.getData(fields).keySet()) {
				String d1 = r1.getData(fields).get(col).convertToString();
				String d2 = r2.getData(ob.fields).get(col).convertToString();
				
				if (d1 != d2) {				
					if (!d1.equals(r2.getData(ob.fields).get(col))) {
						System.out.println(DBGitLang.getInstance().getValue("general", "meta", "diffDataRow").
								withParams(rowHash, col, r1.getData(fields).get(col).toString(), r2.getData(ob.fields).get(col).toString()));
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
			for (ICellData cd : rd.getData(fields).values()) {
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
			for (ICellData cd : rd.getData(fields).values()) {
				count += cd.removeFromGit();
			}			
		}
		
		return count;
	}

    public List<String> getFields() {
        return fields;
    }

    private void warnFilesNotFound(){
		Set<String> filesNotFound = MapFileData.getFilesNotFound();
		if(filesNotFound != null && filesNotFound.size() > 0){
			ConsoleWriter.detailsPrintColor(
				DBGitLang.getInstance().getValue("errors", "dataTable", "filesNotFound")
					.withParams(String.join(";", filesNotFound)), 0, FColor.YELLOW
			);
			filesNotFound.clear();
		}

	}
}
