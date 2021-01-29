package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.mssql.converters.TableConverterMssql;
import ru.fusionsoft.dbgit.mssql.converters.TableDataConverterMssql;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FactoryDbConvertAdapterMssql implements IFactoryDBConvertAdapter {

	private static final Map<String, IDBConvertAdapter> converters;

	static {
		Map<String, IDBConvertAdapter> aMap = new HashMap<String, IDBConvertAdapter>();
		aMap.put(DBGitMetaType.DBGitTable.getValue(), new TableConverterMssql());
		aMap.put(DBGitMetaType.DbGitTableData.getValue(), new TableDataConverterMssql());

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
