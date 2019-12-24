package ru.fusionsoft.dbgit.oracle.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class TableDataConverterOracle implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
		return obj;
	}

}
