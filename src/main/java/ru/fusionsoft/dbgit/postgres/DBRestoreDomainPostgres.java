package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBDomain;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaDomain;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreDomainPostgres extends DBRestoreAdapter {
    @Override
    public final boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        final IDBAdapter adapter = getAdapter();
        final Connection connect = adapter.getConnection();
        try (final StatementLogging st = new StatementLogging(
            connect,
            adapter.getStreamOutputSqlCommand(),
            adapter.isExecSql()
        )) {
            if (! ( obj instanceof MetaDomain )) {
                throw new ExceptionDBGitRestore(
                    lang.getValue("errors", "restore", "metaTypeError").withParams(
                        obj.getName(),
                        "domain",
                        obj.getType().getValue()
                    )
                );
            }
            final DBSQLObject restoreDomain = (DBSQLObject) obj.getUnderlyingDbObject();
            final Map<String, DBDomain> domains = adapter.getDomains(restoreDomain.getSchema());

            if (domains.containsKey(restoreDomain.getName())) {
                final DBDomain currentDomain = domains.get(restoreDomain.getName());
                if (
                    ! restoreDomain.getOptions().get("attributes").equals(
                        currentDomain.getOptions().get("attributes")
                    )
                ) {
                    st.execute(MessageFormat.format(
                        "DROP DOMAIN IF EXISTS {0}._deprecated_{1} RESTRICT;\n" 
                        + "ALTER DOMAIN {0}.{1} RENAME TO _deprecated_{1};\n"
                        + "{2}",
                        currentDomain.getSchema(), currentDomain.getName(), getDdlEscaped(restoreDomain)
                    ));
                } else {
                    if (! DBGitConfig.getInstance().getToIgnoreOnwer(false)) {
                        st.execute(getChangeOwnerDdl(currentDomain, restoreDomain.getOwner()));
                    }
                }
            } else {
                st.execute(getDdlEscaped(restoreDomain));
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
                "DROP DOMAIN IF EXISTS {0}.{1} RESTRICT",
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
                "CREATE DOMAIN " + schema + "." + name,
                "CREATE DOMAIN " + schemaEscaped + "." + nameEscaped
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
