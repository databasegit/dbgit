package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreSequenceOracle extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint("Restoring sequence " + obj.getName() + "...", 1);
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
							
							if(!restoreSeq.getSequence().getOptions().get("order_flag").equals(seq.getOptions().get("order_flag"))) {
								if(restoreSeq.getSequence().getOptions().get("order_flag").equals("Y")) {
									query+="alter sequence "+sequence + " cycle;\n";
								}
								else {
									query+="alter sequence "+sequence + " nocycle;\n";
								}															
							}
							
							if(!restoreSeq.getSequence().getOptions().get("increment_by").equals(seq.getOptions().get("increment_by"))) {
								query+="alter sequence "+sequence+ " increment by "+restoreSeq.getSequence().getOptions().get("increment_by")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("last_number").equals(seq.getOptions().get("last_number"))) {
								query+="alter sequence "+sequence+" start with "+restoreSeq.getSequence().getOptions().get("last_number")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("min_value").equals(seq.getOptions().get("min_value"))) {
								query+="alter sequence "+sequence+ " minvalue "+restoreSeq.getSequence().getOptions().get("min_value")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("max_value").equals(seq.getOptions().get("max_value"))) {
								query+="alter sequence "+sequence + " maxvalue "+restoreSeq.getSequence().getOptions().get("max_value")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("cache_size").equals(seq.getOptions().get("cache_size"))) {
								query+="alter sequence "+sequence + " cache "+restoreSeq.getSequence().getOptions().get("cache_size")+";\n";
							}
							
							/*if(!restoreSeq.getSequence().getOptions().get("owner").equals(seq.getOptions().get("owner"))) {
								query+="alter sequence "+sequence+" owner to "+restoreSeq.getSequence().getOptions().get("owner")+";\n";
							}*/
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
					query += restoreSeq.getSequence().getOptions().get("ddl");
				st.execute(query);		
					//TODO Восстановление привилегий	
				}
				ConsoleWriter.detailsPrintlnGreen("OK");
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed("FAIL");
				throw new ExceptionDBGitRestore("Error restore: Unable to restore Sequences.");
			}			
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed("FAIL");
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}				
		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
