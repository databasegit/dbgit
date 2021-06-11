package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBEnum;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaEnum;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreEnumPostgres extends DBRestoreAdapter {
    @Override
    public final boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        final IDBAdapter adapter = getAdapter();
        final Connection connect = adapter.getConnection();
        try (StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql())) {
            if (! ( obj instanceof MetaEnum )) {
                throw new ExceptionDBGitRestore(
                    lang.getValue("errors", "restore", "metaTypeError")
                    .withParams(obj.getName(), "enum", obj.getType().getValue())
                );
            }
            final DBSQLObject restoreEnum = (DBSQLObject) obj.getUnderlyingDbObject();
            final Map<String, DBEnum> enums = adapter.getEnums(restoreEnum.getSchema());
            
            if(enums.containsKey(restoreEnum.getName())) {
                DBEnum currentEnum = enums.get(restoreEnum.getName());
                if(
                    ! restoreEnum.getOptions().get("elements").equals(
                        currentEnum.getOptions().get("elements")
                    )
                ) {
                    st.execute(MessageFormat.format(
                        "ALTER TYPE {0}.{1} RENAME TO _deprecated_{1};\n{2}",
                        currentEnum.getSchema(), currentEnum.getName(), getDdlEscaped(restoreEnum)
                    ));
                } else{
                    if (! DBGitConfig.getInstance().getToIgnoreOnwer(false)) {
                        st.execute(getChangeOwnerDdl(currentEnum, restoreEnum.getOwner()));
                    }
                }
            } else {
                st.execute(getDdlEscaped(restoreEnum));
            }
           
        } catch (Exception e) {
            throw new ExceptionDBGitRestore(
                lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), 
                e
            );
        } finally {
            ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
        }
        return true;
    }

    @Override
    public void removeMetaObject(IMetaObject obj) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        
        try (StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql())){
            
            final DBSQLObject currentObject = (DBSQLObject) obj.getUnderlyingDbObject();
            st.execute(MessageFormat.format(
                "DROP TYPE {0}.{1}",   
                adapter.escapeNameIfNeeded(getPhisicalSchema(currentObject.getSchema())),
                adapter.escapeNameIfNeeded(currentObject.getName())
            ));
            
        } catch (Exception e) {
            throw new ExceptionDBGitRestore(
                lang.getValue("errors", "restore", "objectRemoveError")
                .withParams(obj.getName()), e);
        } 
    }

    private String getDdlEscaped(DBSQLObject dbsqlObject) {
        String query = dbsqlObject.getSql();
        final String name = dbsqlObject.getName();
        final String schema = dbsqlObject.getSchema();
        final String nameEscaped = adapter.escapeNameIfNeeded(name);
        final String schemaEscaped = adapter.escapeNameIfNeeded(schema);

        if (! name.equalsIgnoreCase(nameEscaped)) {
            query = query.replace(
                "CREATE TYPE " + schema + "." + name,
                "CREATE TYPE " + schemaEscaped + "." + nameEscaped
            );
        }
        if (! query.endsWith(";")) query = query + ";\n";
        query = query + "\n";
        return query;
    }

    private String getChangeOwnerDdl(DBSQLObject dbsqlObject, String owner) {
        return MessageFormat.format("ALTER TYPE {0}.{1} OWNER TO {2}\n"
            , adapter.escapeNameIfNeeded(dbsqlObject.getSchema())
            , adapter.escapeNameIfNeeded(dbsqlObject.getName())
            , owner
        );
    }
}
