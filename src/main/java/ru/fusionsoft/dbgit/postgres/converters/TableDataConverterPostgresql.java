package ru.fusionsoft.dbgit.postgres.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class TableDataConverterPostgresql implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(String dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
		return obj;
	}

}
