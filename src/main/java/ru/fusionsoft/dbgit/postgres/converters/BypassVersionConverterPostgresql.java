package ru.fusionsoft.dbgit.postgres.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.meta.IMetaObject;

import java.text.MessageFormat;

public class BypassVersionConverterPostgresql implements IDBConvertAdapter {
    @Override
    public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
        DbType objDbType = obj.getDbType();
        if (dbType == objDbType){
            return obj;
        } else {
            throw new ExceptionDBGit(MessageFormat.format("Cannot convert {0} from other db type ({1} {2})"
                , obj.getName()
                , obj.getDbType()
                , obj.getDbVersionNumber()
            ));
        }
    }
}
