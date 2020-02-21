package ru.fusionsoft.dbgit.mysql.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class TableDataConverterMySql implements IDBConvertAdapter {

    @Override
    public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
        return obj;
    }

}
