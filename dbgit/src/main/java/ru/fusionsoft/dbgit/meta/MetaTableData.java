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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.data_table.TreeMapRowData;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableData;
import ru.fusionsoft.dbgit.utils.CalcHash;

/**
 * Meta class for Table data
 * @author mikle
 *
 */public class MetaTableData extends MetaBase {
	 protected DBTable table = null;
	 private DBTableData dataTable = null;
	 
	 private TreeMapRowData mapRows = null;

	 public MetaTableData() {}
	 
	 public MetaTableData(DBTable tbl) {
		 setTable(tbl);
	 }
	 
	
	public DBTable getTable() {
		return table;
	}

	public void setTable(DBTable table) {
		this.table = table;
		setName(table.getSchema()+"/"+table.getName()+"."+getType().getValue());
	}

	
	
	@Override
	public void setName(String name) {
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
				.withRecordSeparator("\n")
				.withDelimiter(';')
				.withNullString("<!NULL!>")
				.withQuote('"')
				.withQuoteMode(QuoteMode.ALL)
				;	
	}
	
	@Override
	public void serialize(OutputStream stream) throws Exception {
		Integer count = 0;
		Set<String> fields = null;
		
		CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(stream), getCSVFormat());
		
		for (RowData rd : mapRows.values()) {
			if (count == 0) {
				fields = rd.getData().keySet();
				csvPrinter.printRecord(fields);				
			}
						
			csvPrinter.printRecord(rd.getData().values());

			count++;
		}
		csvPrinter.close();
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws Exception {
		
		MetaTable metaTable = getMetaTable();		
		List<String> idColumns = metaTable.getIdColumns();
		
		CSVParser csvParser = new CSVParser(new InputStreamReader(stream), getCSVFormat());
		List<CSVRecord> csvRecords = csvParser.getRecords(); 
		
		if (csvRecords.size() > 0) {
			CSVRecord titleColumns = csvRecords.get(0);
				
			mapRows = new TreeMapRowData(); 
			
			for (int i = 1; i < csvRecords.size(); i++) {	
				CSVRecord record = csvRecords.get(i);
				RowData rd = new RowData(record, idColumns, titleColumns);
				mapRows.put(rd);			
			}
		}

		csvParser.close();
		
		return this;
	}
	
	public MetaTable getMetaTable() throws ExceptionDBGit {
		String metaTblName = table.getSchema()+"/"+table.getName()+"."+DBGitMetaType.DBGitTable.getValue();
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		
		IMapMetaObject dbObjs = gmdm.getCacheDBMetaData();
		MetaTable metaTable = (MetaTable) dbObjs.get(metaTblName);
		if (metaTable == null ) {
			metaTable = new MetaTable();
			metaTable.loadFromDB(table);
		}
		return metaTable;
	}

	@Override
	public void loadFromDB() throws ExceptionDBGit {	
		try {			
			IDBAdapter adapter = AdapterFactory.createAdapter();
						
			MetaTable metaTable = getMetaTable();		
			List<String> idColumns = metaTable.getIdColumns();
			
			dataTable = adapter.getTableData(table, IDBAdapter.LIMIT_FETCH);
			
			ResultSet rs = dataTable.getResultSet();
			
			mapRows = new TreeMapRowData(); 
			
			while(rs.next()){
				RowData rd = new RowData(rs, idColumns);
				mapRows.put(rd);
			}
		} catch (Exception e) {
			if (e instanceof ExceptionDBGit) 
				throw (ExceptionDBGit)e;
			throw new ExceptionDBGit(e);
		}

	}


	@Override
	public String getHash() {
		CalcHash ch = new CalcHash();
		if (mapRows == null) 
			return EMPTY_HASH;
		
		for (RowData rd : mapRows.values()) {
			ch.addData(rd.getHashRow());
		}
				
		return ch.calcHashStr();
	}

}
