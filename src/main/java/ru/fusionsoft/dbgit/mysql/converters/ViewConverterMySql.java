package ru.fusionsoft.dbgit.mysql.converters;

import ru.fusionsoft.dbgit.adapters.IDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaView;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class ViewConverterMySql implements IDBConvertAdapter {
    @Override
    public IMetaObject convert(DbType dbType, String dbVersion, IMetaObject obj) throws ExceptionDBGit {
        //FIXME: Если не указана схема для таблицы, то она берётся из конфига, при этом она может не быть основной (например, public в pg) и ещё не создана
        DbType objDbType = obj.getDbType();
        if (dbType == objDbType)
            return obj;
        if (obj instanceof MetaView) {
            MetaView view = (MetaView) obj;
            view.setDbType(DbType.MYSQL);
            ConsoleWriter.println("Processing view " + view.getName());
            switch(objDbType) {
                case POSTGRES:
                    String  ddl = view.getSqlObject().getSql();
                    ddl = ddl.replace("\"", "`").replace("\\`", "\\\"").replace("\n", " ");
                    ddl = "CREATE OR REPLACE VIEW " + view.getSqlObject().getSchema() + ".`" + view.getSqlObject().getName() + "`" + ddl.substring(ddl.toLowerCase().indexOf(" as "));
                    view.getSqlObject().setSql(ddl);
                    return view;
                case ORACLE:

                case MSSQL:

                default:
                    throw new ExceptionDBGit("Cannot convert " + obj.getName());//FIXME
            }
        } else {
            throw new ExceptionDBGit("Cannot convert " + obj.getName());
        }
    }
}
