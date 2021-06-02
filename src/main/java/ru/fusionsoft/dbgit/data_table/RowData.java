package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.*;

import de.siegmar.fastcsv.reader.CsvRow;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class RowData {
	//protected Map<String, ICellData> data = new LinkedHashMap<>();
	private List<ICellData> rowList = new ArrayList<>();
	private String hashRow;
	//protected String key;
	//protected MetaTable metaTable;

	public RowData(ResultSet rs, MetaTable metaTable) throws Exception {
		//this.metaTable = metaTable;
		loadDataFromRS(rs, metaTable);
	}

	public RowData(CsvRow record, MetaTable metaTable, CsvRow titleColumns) throws Exception {
		//this.metaTable = metaTable;
		loadDataFromCSVRecord(record, titleColumns, metaTable);
	}

	@Deprecated
	public RowData(CSVRecord record, MetaTable metaTable, CSVRecord titleColumns) throws Exception {
		//this.metaTable = metaTable;
		loadDataFromCSVRecord(record, titleColumns, metaTable);
	}

	private void loadDataFromRS(ResultSet rs, MetaTable metaTable) throws Exception {
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			String columnName = rs.getMetaData().getColumnName(i+1);

			if (columnName.equalsIgnoreCase("DBGIT_ROW_NUM"))
				continue;

			ICellData cd = FactoryCellData.createCellData(metaTable.getFieldsMap().get(columnName).getTypeUniversal());

			if (cd.loadFromDB(rs, rs.getMetaData().getColumnName(i+1)))
				rowList.add(cd);
				//data.put(columnName, cd);
		}

		hashRow = calcRowHash().substring(0, 24);

		//key = calcRowKey(metaTable.getIdColumns());
	}

	private void loadDataFromCSVRecord(CsvRow record, CsvRow titleColumns, MetaTable metaTable) throws Exception {
		if (record.getFieldCount() != titleColumns.getFieldCount()) {
			throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "dataTable", "differentCount"));
		}

		for (int i = 0; i < record.getFieldCount(); i++) {
			String columnName = titleColumns.getField(i);
			if (metaTable.getFieldsMap().get(columnName) == null) {
				throw new ExceptionDBGitRunTime(DBGitLang.getInstance().getValue("errors", "dataTable", "fieldNotFound").withParams(columnName));
			}

			ICellData cd = FactoryCellData.createCellData(metaTable.getFieldsMap().get(columnName).getTypeUniversal());
			cd.deserialize(record.getField(i).equals("<!NULL!>") ? null : record.getField(i));

			rowList.add(cd);
			//data.put(columnName, cd);
		}
		hashRow = calcRowHash().substring(0, 24);

		//key = calcRowKey(metaTable.getIdColumns());

	}

	@Deprecated
	private void loadDataFromCSVRecord(CSVRecord record, CSVRecord titleColumns, MetaTable metaTable) throws Exception {

		if (record.size() != titleColumns.size()) {
			throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "dataTable", "differentCount"));
		}		

		for (int i = 0; i < record.size(); i++) {	
			String columnName = titleColumns.get(i);
			if (metaTable.getFieldsMap().get(columnName) == null) {
				throw new ExceptionDBGitRunTime(DBGitLang.getInstance().getValue("errors", "dataTable", "fieldNotFound").withParams(columnName)); 
			}

			ICellData cd = FactoryCellData.createCellData(metaTable.getFieldsMap().get(columnName).getTypeUniversal());
			cd.deserialize(record.get(i));

			rowList.add(cd);
			//data.put(columnName, cd);
		}
		hashRow = calcRowHash().substring(0, 24);
		
		//key = calcRowKey(metaTable.getIdColumns());
	}
	
	public void saveDataToCsv(CSVPrinter csvPrinter, DBTable tbl) throws Exception {
		//for (ICellData cd : getData().values())
        for (ICellData cd : rowList)
			csvPrinter.print(cd.serialize(tbl));
		
		csvPrinter.println();
	}
	/*
	public String calcRowKey(List<String> idColumns) throws Exception {
		if (idColumns.size() > 0) {
			StringBuilder keyBuilder = new StringBuilder();
			for (String nmId : idColumns) {
				keyBuilder.append(data.get(nmId).convertToString()+"_");
			}
			return keyBuilder.toString();
		} else {
			return hashRow;
		}
	}
	*/
    public String calcRowKey(List<Integer> idColumns) throws Exception {
        return hashRow;
        /*
        if (idColumns.size() > 0) {
            StringBuilder keyBuilder = new StringBuilder();
            for (Integer nmId : idColumns) {
                keyBuilder.append(rowList.get(nmId).convertToString()+"_");
            }
            return keyBuilder.toString();
        } else {
            return hashRow;
        }*/
    }


	public String calcRowHash() throws Exception {
		CalcHash ch = new CalcHash();
		//for (ICellData cd : data.values()) {
        for (ICellData cd : rowList) {
			String str = cd.convertToString();
			if ( str != null)
				ch.addData(str);
		}
		return ch.calcHashStr();
	}

	public Map<String, ICellData> getData(List<String> fields) {
		Map<String, ICellData> res = new LinkedHashMap<>();

		int i = 0;
		for (ICellData cd : rowList) {
		    res.put(fields.get(i), cd);
		    i++;
        }

		return  res;
	}

    public Map<String, ICellData> getData() {
        Map<String, ICellData> res = new HashMap<>();

        int i = 0;
        for (ICellData cd : rowList) {
            res.put(String.valueOf(i), cd);
            i++;
        }

        return  res;
    }

    public List<ICellData> getListData() {
		return rowList;
	}

	public String getHashRow() {
		return hashRow;
	}

	public String getKey() {
		//return key;
        return hashRow;
	}
	
	
}
