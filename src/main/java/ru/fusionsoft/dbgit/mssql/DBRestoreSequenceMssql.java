package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;

public class DBRestoreSequenceMssql extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaSequence) {

				MetaSequence restoreSeq = (MetaSequence)obj;
				String seqSchema = restoreSeq.getSequence().getSchema();
				StringProperties props = restoreSeq.getSequence().getOptions();
				String seqName = props.get("name").getData();
				String sequenceSam = seqSchema+"."+seqName;


				//check existence
				Map<String, DBSequence> seqs = adapter.getSequences(seqSchema);
				boolean exist = false;
				for(DBSequence seq:seqs.values()){
					String currentName = seq.getOptions().getChildren().get("name").getData();
					if(currentName.equalsIgnoreCase(seqName)){
						exist = true;
						break;
					}
				}

				String ddl = exist
					? ("ALTER SEQUENCE " + sequenceSam)
					: ("CREATE SEQUENCE " + props.get("name").getData());

				ddl += " AS " + props.get("typename").getData()
					+ " START WITH " + props.get("start_value").getData()
					+ " INCREMENT BY " + props.get("increment").getData()
					+ ( Objects.nonNull(props.get("minimum_value"))
						? " MINVALUE " + props.get("minimum_value").getData()
						: " NO MINVALUE "
					)
					+ (Objects.nonNull(props.get("maximum_value"))
						? " MAXVALUE " + props.get("maximum_value").getData()
						: " NO MAXVALUE "
					)
					+ ((props.get("is_cached").getData().equals("1"))
						? " CACHE " + (props.get("cache_size") != null
							? props.get("cache_size").getData() : " " )
						: " NO CACHE")
					+ ((props.get("is_cycling").getData().equals("1"))
						? " CYCLE "
						: " NO CYCLE "
					+ "\n");

				ddl += MessageFormat.format(
					"ALTER SCHEMA {0} TRANSFER {1}.{2}",
					adapter.getConnection().getSchema(),
					props.get("owner").getData(),
					props.get("name").getData()
				);

				//TODO Восстановление привилегий
				st.execute(ddl);
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
