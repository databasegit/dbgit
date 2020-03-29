package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaUser;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Objects;

public class DBRestoreUserMssql extends DBRestoreAdapter{

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreUser").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaUser) {
				MetaUser usr = (MetaUser)obj;
				StringProperties opts = usr.getObjectOption().getOptions();
				StringProperties ddl = opts.get("ddl");
				boolean isMssqlDdl = Objects.nonNull(ddl) && ddl.getData().contains("CREATE LOGIN");

				if(isMssqlDdl) {  st.execute(ddl.getData()); } else {

					StringProperties loginName = opts.get("loginName");
					StringProperties userName = opts.get("userName");
					StringProperties passwordHash = opts.get("passwordHash");
					StringProperties isDisabledLogin = opts.get("isDisabledLogin");
					StringProperties defaultSchema = opts.get("schemaName");

					String loginNameActual = (loginName != null) ? loginName.getData() : usr.getName();
					String userNameActual = (userName != null) ? userName.getData() : usr.getName();

					String withPasswordTerm = (passwordHash != null)
							? MessageFormat.format("WITH PASSWORD = {0} HASHED", passwordHash.getData()) : "";

					String grantConnectTerm = (isDisabledLogin == null || isDisabledLogin.getData().equals("0"))
							? MessageFormat.format("GRANT CONNECT SQL TO [{0}];", loginNameActual) : "";

					String withDefaultSchemaTerm = (defaultSchema != null)
							? MessageFormat.format("WITH DEFAULT SCHEMA {0}", defaultSchema.getData()) : "";

					String createLoginTerm = MessageFormat.format("CREATE LOGIN [{0}] {1}; {2} CREATE USER [{3}] FOR LOGIN {0} {4};",
						loginNameActual,
						withPasswordTerm,
						grantConnectTerm,
						userNameActual,
						withDefaultSchemaTerm
					);
					st.execute(createLoginTerm);
				}
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}
		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
