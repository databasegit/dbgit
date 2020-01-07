package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaRole;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.util.Map;

public class DBRestoreRoleMssql extends DBRestoreAdapter{

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreRole").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaRole) {
				MetaRole restoreRole = (MetaRole)obj;
				Map<String, DBRole> roles = adapter.getRoles();
				boolean exist = false;
				if(!(roles.isEmpty() || roles == null)) {
					for(DBRole role:roles.values()) {
						if(restoreRole.getObjectOption().getName().equals(role.getName())){
							exist = true;
							//String test1 = changedsch.getObjectOption().getName();
							// TODO MSSQL restore MetaRole script
							String rolbypassrls = restoreRole.getObjectOption().getOptions().getChildren().get("rolbypassrls").getData();
							if(!role.getOptions().getChildren().get("rolbypassrls").getData().equals(rolbypassrls)) {
								if(rolbypassrls.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" BYPASSRLS");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOBYPASSRLS");
								}
							}

							String rolcanlogin = restoreRole.getObjectOption().getOptions().getChildren().get("rolcanlogin").getData();
							if(!role.getOptions().getChildren().get("rolcanlogin").getData().equals(rolcanlogin)) {
								if(rolcanlogin.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" LOGIN");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOLOGIN");
								}

							}

							String rolconnlimit = restoreRole.getObjectOption().getOptions().getChildren().get("rolconnlimit").getData();
							if(!role.getOptions().getChildren().get("rolconnlimit").getData().equals(rolconnlimit)) {
								st.execute("ALTER ROLE "+ role.getName() +" CONNECTION LIMIT " + rolconnlimit);

							}

							String rolcreatedb = restoreRole.getObjectOption().getOptions().getChildren().get("rolcreatedb").getData();
							if(!role.getOptions().getChildren().get("rolcreatedb").getData().equals(rolcreatedb)) {
								if(rolcreatedb.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" CREATEDB");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOCREATEDB");
								}

							}

							String rolcreaterole = restoreRole.getObjectOption().getOptions().getChildren().get("rolcreaterole").getData();
							if(!role.getOptions().getChildren().get("rolcreaterole").getData().equals(rolcreaterole)) {
								if(rolcreaterole.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" CREATEROLE");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOCREATEROLE");
								}

							}

							String rolinherit = restoreRole.getObjectOption().getOptions().getChildren().get("rolinherit").getData();
							if(!role.getOptions().getChildren().get("rolinherit").getData().equals(rolinherit)) {
								if(rolinherit.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" INHERIT");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOINHERIT");
								}

							}

							String rolreplication = restoreRole.getObjectOption().getOptions().getChildren().get("rolreplication").getData();
							if(!role.getOptions().getChildren().get("rolreplication").getData().equals(rolreplication)) {
								if(rolreplication.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" REPLICATION");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOREPLICATION");
								}

							}

							String rolsuper = restoreRole.getObjectOption().getOptions().getChildren().get("rolsuper").getData();
							if(!role.getOptions().getChildren().get("rolsuper").getData().equals(rolsuper)) {
								if(rolsuper.equals("t")) {
									st.execute("ALTER ROLE "+ role.getName() +" SUPERUSER");
								}
								else {
									st.execute("ALTER ROLE "+ role.getName() +" NOSUPERUSER");
								}

							}

							if(restoreRole.getObjectOption().getOptions().getChildren().containsKey("rolvaliduntil")) {
								st.execute("ALTER ROLE "+ role.getName() +" VALID UNTIL " +restoreRole.getObjectOption().getOptions().getChildren().get("rolvaliduntil").getData());
							}



						}
						//TODO Восстановление привилегий
					}
				}

				if(!exist){
					//TODO MSSQL creating Role names
					String rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolcanlogin,rolreplication,rolbypassrls;
					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolsuper").getData().equals("t")) {
						rolsuper = "SUPERUSER";
					}
					else {
						rolsuper = "NOSUPERUSER";
					}

					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolcreatedb").getData().equals("t")) {
						rolcreatedb = "CREATEDB";
					}
					else {
						rolcreatedb = "NOCREATEDB";
					}

					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolcreaterole").getData().equals("t")) {
						rolcreaterole = "CREATEROLE";
					}
					else {
						rolcreaterole = "NOCREATEROLE";
					}

					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolinherit").getData().equals("t")) {
						rolinherit = "INHERIT";
					}
					else {
						rolinherit = "NOINHERIT";
					}

					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolcanlogin").getData().equals("t")) {
						rolcanlogin = "LOGIN";
					}
					else {
						rolcanlogin = "NOLOGIN";
					}

					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolreplication").getData().equals("t")) {
						rolreplication = "REPLICATION";
					}
					else {
						rolreplication = "NOREPLICATION";
					}

					if(restoreRole.getObjectOption().getOptions().getChildren().get("rolbypassrls").getData().equals("t")) {
						rolbypassrls = "BYPASSRLS";
					}
					else {
						rolbypassrls = "NOBYPASSRLS";
					}

					st.execute("CREATE ROLE "+ restoreRole.getObjectOption().getName()+
							" " + rolsuper+
							" " + rolcreatedb+
							" " + rolcreaterole+
							" " + rolinherit+
							" " + rolcanlogin+
							" " + rolreplication+
							" " + rolbypassrls+
							" CONNECTION LIMIT " +restoreRole.getObjectOption().getOptions().getChildren().get("rolconnlimit").getData());
					if(restoreRole.getObjectOption().getOptions().getChildren().containsKey("rolvaliduntil")) {
						st.execute("ALTER ROLE "+ restoreRole.getObjectOption().getName() +" VALID UNTIL " +restoreRole.getObjectOption().getOptions().getChildren().get("rolvaliduntil").getData());
					}
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
