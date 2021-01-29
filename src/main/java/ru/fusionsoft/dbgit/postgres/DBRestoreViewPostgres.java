package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.text.MessageFormat;
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
		try {
			if (obj instanceof MetaView) {
				MetaView restoreView = (MetaView)obj;
				Map<String, DBView> views = adapter.getViews(restoreView.getSqlObject().getSchema());
				boolean exist = false;
				if(! (views == null || views.isEmpty())) {
					for(DBView vw:views.values()) {
						if(restoreView.getSqlObject().getName().equals(vw.getName())){
							exist = true;
							if(!restoreView.getSqlObject().getSql().equals(vw.getSql())) {
								st.execute(getDdlEscaped(restoreView));
							}
							if(!restoreView.getSqlObject().getOwner().equals(vw.getOwner())) {
								st.execute(getChangeOwnerDdl(restoreView, restoreView.getSqlObject().getOwner()));
							}
						}
					}
				}
				if(!exist){
					String query = getDdlEscaped(restoreView) + getChangeOwnerDdl(restoreView, restoreView.getSqlObject().getOwner());
					st.execute(query);
				}
				//TODO Восстановление привилегий ?
			}
			else
			{
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "view", obj.getType().getValue()
                ));
			}			
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError")
				.withParams(obj.getName())
				, e
			);
		} finally {
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
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
			st.execute("DROP VIEW "+adapter.escapeNameIfNeeded(schema)+"."+adapter.escapeNameIfNeeded(vw.getName()));
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()),0);
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

	private String getDdlEscaped(MetaView view){
		String name = view.getSqlObject().getName();
		String schema = view.getSqlObject().getSchema();
		String query = view.getSqlObject().getSql();
		String nameEscaped = adapter.escapeNameIfNeeded(name);

		if (!name.equalsIgnoreCase(nameEscaped)) {
			query = query.replace(
				"create or replace view " + schema + "." + name,
				"create or replace view " + schema + "." + nameEscaped
			);
		}
		if (!query.endsWith(";")) query = query + ";\n";
		query = query + "\n";
		return query;
	}

	private String getChangeOwnerDdl(MetaView view, String owner){
		return  MessageFormat.format("ALTER VIEW {0}.{1} OWNER TO {2}\n"
			, view.getSqlObject().getSchema()
			, adapter.escapeNameIfNeeded(view.getSqlObject().getName())
			, owner
		);
	}
}
