package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaFunction;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreSequencePostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreSeq").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaSequence) {
				MetaSequence restoreSeq = (MetaSequence)obj;
				Map<String, DBSequence> seqs = adapter.getSequences(((MetaSequence) obj).getSequence().getSchema());
				boolean exist = false;
				if(!(seqs.isEmpty() || seqs == null)) {
					for(DBSequence seq:seqs.values()) {
						if(restoreSeq.getSequence().getName().equals(seq.getName())){
							String query="";
							exist = true;
							String sequence = restoreSeq.getSequence().getSchema()+ "." +restoreSeq.getSequence().getName();
							if(!restoreSeq.getSequence().getOptions().get("cycle_option").equals(seq.getOptions().get("cycle_option"))) {
								if(restoreSeq.getSequence().getOptions().get("cycle_option").equals("YES")) {
									query+="alter sequence "+sequence + " cycle;\n";
								}
								else {
									query+="alter sequence "+sequence + " no cycle;\n";
								}
							}

							if(!restoreSeq.getSequence().getOptions().get("increment").equals(seq.getOptions().get("increment"))) {
								query+="alter sequence "+sequence+ " increment "+restoreSeq.getSequence().getOptions().get("increment")+";\n";
							}

							if(!restoreSeq.getSequence().getOptions().get("start_value").equals(seq.getOptions().get("start_value"))) {
								query+="alter sequence "+sequence+" start "+restoreSeq.getSequence().getOptions().get("start_value")+";\n";
							}

							if(!restoreSeq.getSequence().getOptions().get("minimum_value").equals(seq.getOptions().get("minimum_value"))) {
								query+="alter sequence "+sequence+ " minvalue "+restoreSeq.getSequence().getOptions().get("minimum_value")+";\n";
							}

							if(!restoreSeq.getSequence().getOptions().get("maximum_value").equals(seq.getOptions().get("maximum_value"))) {
								query+="alter sequence "+sequence + " maxvalue "+restoreSeq.getSequence().getOptions().get("maximum_value")+";\n";
							}

							if(!restoreSeq.getSequence().getOptions().get("owner").equals(seq.getOptions().get("owner"))) {
								query+="alter sequence "+sequence+" owner to "+restoreSeq.getSequence().getOptions().get("owner")+";\n";
							}
							if(query.length()>1) {
								st.execute(query);
							}
							//TODO Восстановление привилегий
						}
					}
				}
				if(!exist){
					String query="";
					String seqName = restoreSeq.getSequence().getName();
					String schema = restoreSeq.getSequence().getSchema();
					if(restoreSeq.getSequence().getOptions().get("cycle_option").equals("YES")){
						query+="create sequence \"" + schema + "\".\"" + seqName+"\"" +
								"cycle \n"+
								"increment" + restoreSeq.getSequence().getOptions().get("increment")+"\n"+
								"start " + restoreSeq.getSequence().getOptions().get("start_value")+"\n"+
								"minvalue "+ restoreSeq.getSequence().getOptions().get("minimum_value")+"\n"+
								"maxvalue " + restoreSeq.getSequence().getOptions().get("maximum_value")+";\n";
						query+="alter sequence \""+ schema + "\".\"" + seqName+"\" owner to\""+ restoreSeq.getSequence().getOptions().get("owner")+"\";";
					}
					else {
						query+="create sequence \"" + schema + "\".\"" + seqName+"\"" +
								"no cycle \n"+
								"increment " + restoreSeq.getSequence().getOptions().get("increment")+"\n"+
								"start " + restoreSeq.getSequence().getOptions().get("start_value")+"\n"+
								"minvalue "+ restoreSeq.getSequence().getOptions().get("minimum_value")+"\n"+
								"maxvalue " + restoreSeq.getSequence().getOptions().get("maximum_value")+";\n";
						query+="alter sequence \""+ schema + "\".\"" + seqName+"\" owner to\""+ restoreSeq.getSequence().getOptions().get("owner")+"\";";
					}
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
			if(! (obj instanceof MetaSequence)) throw new ExceptionDBGit("Wrong IMetaObject type, expected: seq, was: " + obj.getType().getValue());
			MetaSequence seqMeta = (MetaSequence) obj;
			DBSequence seq = seqMeta.getSequence();
			if (seq == null) return;

			String schema = getPhisicalSchema(seq.getSchema());
			st.execute("DROP SEQUENCE IF EXISTS "+DBAdapterPostgres.escapeNameIfNeeded(schema)+"."+DBAdapterPostgres.escapeNameIfNeeded(seq.getName()));

		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}

	}

}
