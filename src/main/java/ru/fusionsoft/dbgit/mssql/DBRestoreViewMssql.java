package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaView;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.util.Map;

public class DBRestoreViewMssql extends DBRestoreAdapter {

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
							// TODO MSSQL restore View script
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

					if (!query.endsWith(";")) query = query + ";";
					query = query + "\n";

					query+= "ALTER VIEW "+restoreView.getSqlObject().getName() +" OWNER TO "+restoreView.getSqlObject().getOwner()+";\n";
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
		// TODO Auto-generated method stub

	}

}
