package ru.fusionsoft.dbgit.data_table;

import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.FieldType;

public class FactoryCellData {	
	private static Map<FieldType, Class<? extends ICellData>> mapTypes = new HashMap<>();
	
	public static void regMappingTypes(FieldType name, Class<? extends ICellData> cl) {
		mapTypes.put(name, cl);
	}
	
	public static boolean contains(FieldType nm) {
		return mapTypes.containsKey(nm);
	}
			
	public static ICellData createCellData(FieldType typeMapping) throws ExceptionDBGit {
		try {		
			Class<? extends ICellData> cl = mapTypes.get(typeMapping);
			
			return cl.newInstance();
			
		} catch (Exception e) {
			throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "dataTable", "errorCellData").withParams(typeMapping.toString()), e);
		}
	}
}
