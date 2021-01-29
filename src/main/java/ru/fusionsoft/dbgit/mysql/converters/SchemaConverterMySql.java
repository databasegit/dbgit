package ru.fusionsoft.dbgit.mysql.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class SchemaConverterMySql implements IDBConvertAdapter {
    @Override
    public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
        DbType objDbType = obj.getDbType();
        if (dbType == objDbType)
            return obj;
        if (obj instanceof MetaSchema) {
            MetaSchema schema = (MetaSchema) obj;
            schema.setDbType(DbType.MYSQL);
            ConsoleWriter.println(DBGitLang.getInstance()
                .getValue("general", "convert", "convertingSchema")
                .withParams(schema.getName())
                , messageLevel
            );
            //ConsoleWriter.printlnGreen("URL=" + DBConnection.getInstance().dbName);
            switch(objDbType) {
                case POSTGRES:
                    //DBGitConfig.getInstance()
                    if(schema.getName().equals("public"))
                        schema.setName("public");
                    break;
                case ORACLE:
                    break;
                case MSSQL:
                    break;
                default:
                    break;
            }
            return schema;
        } else {
            throw new ExceptionDBGit("Cannot convert " + obj.getName());
        }
    }
}
