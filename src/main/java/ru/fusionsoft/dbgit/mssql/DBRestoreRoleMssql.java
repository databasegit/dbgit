package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaRole;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

public class DBRestoreRoleMssql extends DBRestoreAdapter{

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaRole) {
				MetaRole restoreRole = (MetaRole)obj;
				Map<String, DBRole> roles = adapter.getRoles();
				StringProperties opts = restoreRole.getObjectOption().getOptions();
				String restoreDdl = opts.get("ddl") != null ? opts.get("ddl").getData() : "";
				String restoreRoleName = restoreRole.getObjectOption().getName();
				String simpleCreateRoleDdl = MessageFormat.format("CREATE ROLE [{0}];", restoreRoleName);

				boolean exist = false;

				if(!(roles.isEmpty() || roles == null)) {
					for(DBRole role:roles.values()) {

						if(restoreRole.getObjectOption().getName().equals(role.getName())){
							exist = true;

							String existingDdl = role.getOptions().get("ddl") != null ? role.getOptions().get("ddl").getData() : "";
							boolean isEqualDdls = restoreDdl
								.replaceAll("\\s+", "")
								.equals(existingDdl.replaceAll("\\s+", ""));

							if(!isEqualDdls){
								if(!restoreDdl.isEmpty()) st.execute(restoreDdl);
								else st.execute(simpleCreateRoleDdl);
								//TODO Восстановление привилегий вместо simpleCreateRoleDdl
							}
						}
					}
				}

				if(!exist){
					if(!restoreDdl.isEmpty()) st.execute(restoreDdl);
					else st.execute(simpleCreateRoleDdl);
				}
				connect.commit();
			}
			else {
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
					obj.getName()
					,  "role", obj.getType().getValue()
				));
			}
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"), 0);
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			st.close();
		}
		return true;
	}
	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
