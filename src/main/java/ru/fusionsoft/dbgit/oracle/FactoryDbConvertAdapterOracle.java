package ru.fusionsoft.dbgit.oracle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.oracle.converters.TableConverterOracle;
import ru.fusionsoft.dbgit.oracle.converters.TableDataConverterOracle;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class FactoryDbConvertAdapterOracle implements IFactoryDBConvertAdapter {

	private static final Map<String, IDBConvertAdapter> converters;
	
	static {
        Map<String, IDBConvertAdapter> aMap = new HashMap<String, IDBConvertAdapter>();
        aMap.put(DBGitMetaType.DBGitTable.getValue(), new TableConverterOracle());
        aMap.put(DBGitMetaType.DbGitTableData.getValue(), new TableDataConverterOracle());
        
		converters = Collections.unmodifiableMap(aMap);
	}
	
	@Override
	public IDBConvertAdapter getConvertAdapter(String objectType) throws Exception {
		if (!converters.containsKey(objectType)) {
			ConsoleWriter.println(DBGitLang.getInstance()
			    .getValue("errors", "convert", "cannotConvert")
			    .withParams(objectType)
			    , 1
			);
			return null;
		} else		
			return converters.get(objectType);

	}

}
