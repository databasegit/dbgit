package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.dbobjects.DBUserDefinedType;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaUDT;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreUDTPostgres extends DBRestoreAdapter {
    @Override
    public final boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        final IDBAdapter adapter = getAdapter();
        final Connection connect = adapter.getConnection();
        try (final StatementLogging st = new StatementLogging(
            connect,
            adapter.getStreamOutputSqlCommand(),
            adapter.isExecSql()
        )) {
            if (! ( obj instanceof MetaUDT )) {
                throw new ExceptionDBGitRestore(
                    lang.getValue("errors", "restore", "metaTypeError").withParams(obj.getName(), "udt", obj.getType().getValue())
                );
            }
            final DBSQLObject restoreUDT = (DBSQLObject) obj.getUnderlyingDbObject();
            final Map<String, DBUserDefinedType> udts = adapter.getUDTs(restoreUDT.getSchema());

            if (udts.containsKey(restoreUDT.getName())) {
                final DBUserDefinedType currentUDT = udts.get(restoreUDT.getName());
                if (
                    ! restoreUDT.getOptions().get("attributes").equals(
                        currentUDT.getOptions().get("attributes")
                    )
                ) {
                    st.execute(MessageFormat.format(
                        "ALTER TYPE {0}.{1} RENAME TO _deprecated_{1};\n" 
                        + "{2}",
                        currentUDT.getSchema(), currentUDT.getName(), getDdlEscaped(restoreUDT)
                    ));
                } else {
                    if (! DBGitConfig.getInstance().getToIgnoreOnwer(false)) {
                        st.execute(getChangeOwnerDdl(currentUDT, restoreUDT.getOwner()));
                    }
                }
            } else {
                st.execute(getDdlEscaped(restoreUDT));
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
        final IDBAdapter adapter = getAdapter();
        final Connection connect = adapter.getConnection();

        try (StatementLogging st = new StatementLogging(
            connect,
            adapter.getStreamOutputSqlCommand(),
            adapter.isExecSql()
        )) {

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
