package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;

public interface IDBConvertAdapter {
	public IMetaObject convert(String dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit;
}
