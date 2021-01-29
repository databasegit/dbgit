package ru.fusionsoft.dbgit.postgres;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.oracle.converters.TableConverterOracle;
import ru.fusionsoft.dbgit.postgres.converters.BypassVersionConverterPostgresql;
import ru.fusionsoft.dbgit.postgres.converters.TableConverterPostgresql;
import ru.fusionsoft.dbgit.postgres.converters.TableDataConverterPostgresql;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class FactoryDbConvertAdapterPostgres implements IFactoryDBConvertAdapter {

	private static final Map<String, IDBConvertAdapter> converters;

	static {
		Map<String, IDBConvertAdapter> aMap = new HashMap<String, IDBConvertAdapter>();
		aMap.put(DBGitMetaType.DBGitTable.getValue(), new TableConverterPostgresql());
		aMap.put(DBGitMetaType.DbGitTableData.getValue(), new TableDataConverterPostgresql());
		aMap.put(DBGitMetaType.DBGitSequence.getValue(), new BypassVersionConverterPostgresql());
//		aMap.put(DBGitMetaType.DBGit.getValue(), new BypassVersionConverterPostgresql());
//		aMap.put(DBGitMetaType.DBGit.getValue(), new BypassVersionConverterPostgresql());
//		aMap.put(DBGitMetaType.DBGit.getValue(), new BypassVersionConverterPostgresql());
//		aMap.put(DBGitMetaType.DBGit.getValue(), new BypassVersionConverterPostgresql());

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
