package ru.fusionsoft.dbgit.data_table;

import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;

public class FactoryCellData {	
	private static Map<String, Class<? extends ICellData>> mapTypes = new HashMap<>();
	
	public static void regMappingTypes(String name, Class<? extends ICellData> cl) {
		mapTypes.put(name, cl);
	}
	
	public static boolean contains(String nm) {
		return mapTypes.containsKey(nm);
	}
			
	public static ICellData createCellData(String typeMapping) throws ExceptionDBGit {
		try {		
			Class<? extends ICellData> cl = mapTypes.get(typeMapping);
			
			return cl.newInstance();
			
		} catch (Exception e) {
			throw new ExceptionDBGit("Error create CellData for type "+typeMapping, e);
		}
	}
}
