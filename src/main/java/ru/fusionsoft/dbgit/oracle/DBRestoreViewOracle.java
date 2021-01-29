package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaView;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreViewOracle extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {	
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
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
								st.execute(restoreView.getSqlObject().getSql());							
							}
							
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					st.execute(restoreView.getSqlObject().getSql());	
					//TODO Восстановление привилегий	
				}
				ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			}
			else
			{
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "view", obj.getType().getValue()
                ));
			}			
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
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
			MetaView vwMeta = (MetaView) obj;			
			st.execute("DROP VIEW " + vwMeta.getSqlObject().getName());

		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

}