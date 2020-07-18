package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaView;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreViewPostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {	
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreView").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaView) {
				MetaView restoreView = (MetaView)obj;								
				Map<String, DBView> views = adapter.getViews(restoreView.getSqlObject().getSchema());
				boolean exist = false;
				if(!(views.isEmpty() || views == null)) {
					for(DBView vw:views.values()) {
						if(restoreView.getSqlObject().getName().equals(vw.getName())){
							exist = true;
							if(!restoreView.getSqlObject().getSql().equals(vw.getSql())) {
								//String ss = "CREATE OR REPLACE VIEW "+restoreView.getSqlObject().getName() +" AS\n"+restoreView.getSqlObject().getSql();
								st.execute(restoreView.getSqlObject().getName() +" AS\n"+restoreView.getSqlObject().getSql()); 							
							}
							
							if(!restoreView.getSqlObject().getOwner().equals(vw.getOwner())) {
								st.execute("ALTER VIEW "+restoreView.getSqlObject().getName() +" OWNER TO "+restoreView.getSqlObject().getOwner()); 							
							}
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					String query = restoreView.getSqlObject().getSql();
					String name = restoreView.getSqlObject().getName();
					boolean nameShouldBeEscaped = name.contains(".") || Character.isUpperCase(name.codePointAt(0));
					if (nameShouldBeEscaped) {
						query = query.replace(
								"create or replace view " + restoreView.getSqlObject().getSchema() + "." + restoreView.getSqlObject().getName(), 
								"create or replace view " + restoreView.getSqlObject().getSchema() + ".\"" + restoreView.getSqlObject().getName() + "\"");
					}
					
					if (!query.endsWith(";")) query = query + ";";
					query = query + "\n";
					
					query+= "ALTER VIEW "+ restoreView.getSqlObject().getSchema() + "."
							+ ( nameShouldBeEscaped  ?( "\"" + name + "\"") : name)
							+ " OWNER TO "+restoreView.getSqlObject().getOwner()+";\n";
					st.execute(query);
					//TODO Восстановление привилегий
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
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if(! (obj instanceof MetaView)) throw new ExceptionDBGit("Wrong IMetaObject type, expected: vw, was: " + obj.getType().getValue());
			MetaView vwMeta = (MetaView) obj;
			DBView vw = (DBView) vwMeta.getSqlObject();
			if (vw == null) return;

			String schema = getPhisicalSchema(vw.getSchema());
			st.execute("DROP VIEW "+DBAdapterPostgres.escapeNameIfNeeded(schema)+"."+DBAdapterPostgres.escapeNameIfNeeded(vw.getName()));
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

}
