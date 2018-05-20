package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreSequencePostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
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
							if(!restoreSeq.getSequence().getOptions().get("cycle_option").equals(seq.getOptions().get("cycle_option"))) {
								if(restoreSeq.getSequence().getOptions().get("cycle_option").equals("YES")) {
									query+="alter sequence \""+restoreSeq.getSequence().getSchema()+ "\".\"" +restoreSeq.getSequence().getName()+"\'" + " cycle;\n";
								}
								else {
									query+="alter sequence \""+restoreSeq.getSequence().getSchema()+"\".\""+restoreSeq.getSequence().getName() + "\"" + " no cycle;\n";
								}															
							}
							
							if(!restoreSeq.getSequence().getOptions().get("increment").equals(seq.getOptions().get("increment"))) {
								query+="alter sequence \""+restoreSeq.getSequence().getSchema()+ "\".\"" +restoreSeq.getSequence().getName()+"\"" + " increment "+restoreSeq.getSequence().getOptions().get("increment")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("start_value").equals(seq.getOptions().get("start_value"))) {
								query+="alter sequence \""+restoreSeq.getSequence().getSchema()+ "\".\"" +restoreSeq.getSequence().getName()+"\"" + " start "+restoreSeq.getSequence().getOptions().get("start_value")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("minimum_value").equals(seq.getOptions().get("minimum_value"))) {
								query+="alter sequence \""+restoreSeq.getSequence().getSchema()+ "\".\"" +restoreSeq.getSequence().getName()+"\"" + " minvalue "+restoreSeq.getSequence().getOptions().get("minimum_value")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("maximum_value").equals(seq.getOptions().get("maximum_value"))) {
								query+="alter sequence \""+restoreSeq.getSequence().getSchema()+ "\".\"" +restoreSeq.getSequence().getName()+"\"" + " maxvalue "+restoreSeq.getSequence().getOptions().get("maximum_value")+";\n";
							}
							
							if(!restoreSeq.getSequence().getOptions().get("owner").equals(seq.getOptions().get("owner"))) {
								query+="alter sequence \""+restoreSeq.getSequence().getSchema()+ "\".\"" +restoreSeq.getSequence().getName()+"\"" + " owner to "+restoreSeq.getSequence().getOptions().get("owner")+";\n";
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
					if(restoreSeq.getSequence().getOptions().get("cycle_option").equals("YES")){
						query+="create sequence \"" + restoreSeq.getSequence().getSchema() + "\".\"" + restoreSeq.getSequence().getName()+"\"" +
								"cycle \n"+
								"increment" + restoreSeq.getSequence().getOptions().get("increment")+"\n"+
								"start " + restoreSeq.getSequence().getOptions().get("start_value")+"\n"+
								"minvalue "+ restoreSeq.getSequence().getOptions().get("minimum_value")+"\n"+
								"maxvalue " + restoreSeq.getSequence().getOptions().get("maximum_value")+";\n";
						query+="alter sequence \""+ restoreSeq.getSequence().getSchema() + "\".\"" + restoreSeq.getSequence().getName()+"\" owner to\""+ restoreSeq.getSequence().getOptions().get("owner")+"\";";
					}
					else {
						query+="create sequence \"" + restoreSeq.getSequence().getSchema() + "\".\"" + restoreSeq.getSequence().getName()+"\"" +
								"no cycle \n"+
								"increment " + restoreSeq.getSequence().getOptions().get("increment")+"\n"+
								"start " + restoreSeq.getSequence().getOptions().get("start_value")+"\n"+
								"minvalue "+ restoreSeq.getSequence().getOptions().get("minimum_value")+"\n"+
								"maxvalue " + restoreSeq.getSequence().getOptions().get("maximum_value")+";\n";
						query+="alter sequence \""+ restoreSeq.getSequence().getSchema() + "\".\"" + restoreSeq.getSequence().getName()+"\" owner to\""+ restoreSeq.getSequence().getOptions().get("owner")+"\";";
					}
				st.execute(query);		
					//TODO Восстановление привилегий	
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore SCHEMAS.");
			}			
		} catch (Exception e) {
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
