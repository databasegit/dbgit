package ru.fusionsoft.dbgit.mssql.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class TableDataConverterMssql implements IDBConvertAdapter {

	@Override
	public IMetaObject convert(String dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
		//TODO MSSQL TableData convert method
		return obj;
	}

}
