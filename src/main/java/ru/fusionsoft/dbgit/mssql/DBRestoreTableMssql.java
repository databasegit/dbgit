package ru.fusionsoft.dbgit.mssql;

import ch.qos.logback.classic.db.names.TableName;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DBRestoreTableMssql extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if(Integer.valueOf(step).equals(0)) {
			restoreTableMssql(obj);
			return false;
		}

		if(Integer.valueOf(step).equals(1)) {
			restoreTableIndexesMssql(obj);
			return false;
		}

		if(Integer.valueOf(step).equals(-1)) {
			restoreTableConstraintMssql(obj);
			return false;
		}

		return true;
	}

	public void restoreTableMssql(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable) obj;
				String tblSchema = getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase());
				String tblName = restoreTable.getTable().getName();
				String tblSam = tblSchema+"."+tblName;

				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "table").withParams(tblSam) + "\n", 1);

				Map<String, DBTable> tables = adapter.getTables(tblSchema);

				if(!isTablePresent(tblSchema ,tblName)){
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "createTable"), 2);

					DBTable table = restoreTable.getTable();

					String ddl = "create table " + tblSchema + "." + table.getName() + " (" +
						restoreTable.getFields().values().stream()
							.sorted(Comparator.comparing(DBTableField::getOrder))
							.map(field -> "[" + field.getName() + "]" + " " + field.getTypeSQL())
							.collect(Collectors.joining(", ")) +
					")";
					st.execute(ddl);

					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				}
				restoreTableFieldsMssql(restoreTable, tblName, tblSchema, st);

				Map<String, DBConstraint> constraints = adapter.getConstraints(tblSchema, tblName);

				if(constraints.keySet().size() > 0) {
					removeTableConstraintsMssql(obj);
				}

				// set primary key
				if (constraints.keySet().size() == 0) {
					restoreTablePksMssql(restoreTable, st);
				}
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

	public void restoreTableIndexesMssql(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreIndex").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				Map<String, DBTable> tables = adapter.getTables(schema);
				boolean exist = false;

				//find current table
				if(!(tables.isEmpty() || tables == null)) {
					for(DBTable currentTable:tables.values()) {
						if(restoreTable.getTable().getName().equals(currentTable.getName())){
							exist = true;

							Map<String, DBIndex> currentIndexes = adapter.getIndexes(schema, currentTable.getName());
							MapDifference<String, DBIndex> diffInd = Maps.difference(restoreTable.getIndexes(), currentIndexes);
							Map<String, DBIndex> restoringIdxsUnique = diffInd.entriesOnlyOnLeft();
							Map<String, DBIndex> existingIdxsUnique = diffInd.entriesOnlyOnRight();
							Map<String, ValueDifference<DBIndex>> mergingIdxs = diffInd.entriesDiffering();

							//restore missing
							if(!restoringIdxsUnique.isEmpty()) {
								for(DBIndex ind : restoringIdxsUnique.values()) {
									st.execute(ind.getSql());
								}
							}

							//drop not matched
							if(!existingIdxsUnique.isEmpty()) {
								for(DBIndex ind:existingIdxsUnique.values()) {
									st.execute("DROP INDEX "+schema+"."+ind.getName());
								}
							}

							//process intersects
							if(!mergingIdxs.isEmpty()) {
								for(ValueDifference<DBIndex> idx : mergingIdxs.values()) {
									//so just drop and create again
									String ddl;
									if(idx.rightValue().getOptions().get("is_unique").getData().equals("0")){
										ddl = MessageFormat.format(
									"DROP INDEX [{2}] ON [{0}].[{1}] ",
											schema, currentTable.getName(), idx.rightValue().getName()
										);
									}
									else{
										ddl = MessageFormat.format(
									"ALTER TABLE [{0}].[{1}] DROP CONSTRAINT [{2}]",
											schema, currentTable.getName(), idx.rightValue().getName()
										);
									}
									st.execute(ddl);
									st.execute(idx.leftValue().getSql());
								}
							}

						}
					}
				}
				if(!exist){
					/*for(DBIndex ind:restoreTable.getIndexes().values()) {
						st.execute(ind.getSql());
					}*/
					String errText = lang.getValue("errors", "meta", "notFound").withParams(obj.getName());
					ConsoleWriter.detailsPrintlnRed(errText);
					throw new ExceptionDBGitRestore(errText);
				}
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}
	}

	private void restoreTableConstraintMssql(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreConstr").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				for(DBConstraint constraint :restoreTable.getConstraints().values()) {
					if(!getIsPk(constraint)) {
						String ddl = constraint.getSql();
						ddl = ddl.replaceFirst("ALTER TABLE\\s+\\[?\\w+\\]?.\\[?\\w+\\]?\\s+ADD\\s+CONSTRAINT\\s+\\[?\\w+\\]?\\s+", "");
						ddl = MessageFormat.format(
					"ALTER TABLE {0}.{1} ADD CONSTRAINT [{2}] {3}",
							schema, restoreTable.getTable().getName(), constraint.getName(), ddl
						)
						.replace(" "+constraint.getSchema()+".", " "+schema+".")
						.replace("REFERENCES ", "REFERENCES " + schema + ".");

						st.execute(ddl);
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
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}
	}

	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			MetaTable tblMeta = (MetaTable)obj;
			DBTable tbl = tblMeta.getTable();

			if (tbl == null) return;

			String schema = getPhisicalSchema(tbl.getSchema());
			schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
			st.execute(
			"IF EXISTS ( SELECT  * FROM sys.tables t WHERE t.name = N'"+tbl.getName()+"' AND t.schema =  "+schema+")\n" +
				"DROP TABLE ["+schema+"].["+tbl.getName()+"];"
			);

		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

	private boolean isTablePresent(String schemaName, String tableName){
		Map<String, DBTable> tables = adapter.getTables(schemaName);
		if(tables != null && !tables.isEmpty()) {
			for(DBTable current:tables.values()) {
				if(tableName.equalsIgnoreCase(current.getName())){
					return true;
				}
			}
		}
		return false;
	}

	private void restoreTableFieldsMssql(MetaTable restoreTable, String tblName, String tblSchema, StatementLogging st) throws SQLException {
		//restore tabl fields
		String tblSam = tblSchema+"."+tblName;
		Map<String, DBTableField> currentFields = adapter.getTableFields(tblSchema, restoreTable.getTable().getName().toLowerCase());
		MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFields);
		Map<String, DBTableField> restoringUniqueFields = diffTableFields.entriesOnlyOnLeft();
		Map<String, DBTableField> existingUniqueFields = diffTableFields.entriesOnlyOnRight();
		Map<String, ValueDifference<DBTableField>> mergingFields = diffTableFields.entriesDiffering();


		if( !restoringUniqueFields.isEmpty()){
			ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addColumns"), 2);

			List<DBTableField> values = restoringUniqueFields.values()
					.stream()
					.sorted(Comparator.comparing(DBTableField::getOrder))
					.collect(Collectors.toList());

			for(DBTableField tblField : values) {
				String fieldDdl = MessageFormat.format("ALTER TABLE {0} ADD [{1}] {2}", tblSam, tblField.getName(), tblField.getTypeSQL());
				st.execute(fieldDdl);
			}

			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		}

		if( !existingUniqueFields.isEmpty()) {
			ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "droppingColumns"), 2);

			for(DBTableField tblField:existingUniqueFields.values()) {
				String fieldDdl = MessageFormat.format("ALTER TABLE {1} DROP COLUMN [{2}]", tblSam, tblField.getName());
				st.execute(fieldDdl);
			}

			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		}

		if(!mergingFields.isEmpty()) {
			ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "modifyColumns"), 2);

			for(ValueDifference<DBTableField> fld : mergingFields.values()) {
				DBTableField oldValue = fld.rightValue();
				DBTableField newValue = fld.leftValue();

				if(!newValue.getName().equals(oldValue.getName())) {
					String fieldDdl = MessageFormat.format(
							"EXEC sp_RENAME '{1}.{2}' , '{3}', 'COLUMN'",
							tblSam, oldValue.getName(), newValue.getName()
					);
					st.execute(fieldDdl);
				}

				//if types differ
				if( !newValue.getTypeSQL().equals(oldValue.getTypeSQL()) ) {
					st.execute("ALTER TABLE "+ tblName +" ALTER COLUMN "+ newValue.getName() +" "+ newValue.getTypeSQL());
				}
			}

			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		}
	}

	private void removeTableConstraintsMssql(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				//String s = "SELECT COUNT(*) as constraintscount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+table.getTable().getSchema()+"."+table.getTable().getName()+"'::regclass";
				//ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraintscount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+table.getTable().getSchema()+"."+table.getTable().getName()+"'::regclass");
				//rs.next();
				//Integer constraintsCount = Integer.valueOf(rs.getString("constraintscount"));
				//if(constraintsCount.intValue()>0) {
				Map<String, DBConstraint> constraints = table.getConstraints();
				for(DBConstraint constrs :constraints.values()) {
					if (!constrs.getConstraintType().equalsIgnoreCase("p"))
						st.execute("alter table "+ schema+"."+table.getTable().getName() +" drop constraint IF EXISTS "+constrs.getName());
				}
				//}
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		}
	}

	private boolean getIsPk(DBConstraint dbConstraint){
		StringProperties prop = dbConstraint.getOptions().get("ispk");
		return prop != null && prop.getData().equals("1");
	}

	private void restoreTablePksMssql(MetaTable restoreTable, StatementLogging st) throws SQLException {
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addPk"), 2);
		String tblSchema = restoreTable.getTable().getSchema();
		String tblName = restoreTable.getTable().getName();
		boolean flagPkCreated = false;

		List<DBConstraint> pkConstraints = Lists.newArrayList(restoreTable.getConstraints().values());
        pkConstraints.removeIf( x-> !getIsPk(x) );
		if(pkConstraints.size() > 1) throw new SQLException();

		for(DBConstraint pk : pkConstraints){

			String ddl = pk.getSql();
			ddl = ddl.replaceFirst("ALTER TABLE\\s+\\[?\\w+\\]?.\\[?\\w+\\]?\\s+ADD\\s+CONSTRAINT\\s+\\[?\\w+\\]?\\s+", "");
			ddl = MessageFormat.format(
					"ALTER TABLE {0}.{1} ADD CONSTRAINT [{2}] {3}",
					tblSchema, tblName, pk.getName(), ddl
			);
			ddl = ddl.replace(" "+pk.getSchema()+".", " "+restoreTable.getTable().getSchema()+".");

			st.execute(ddl);
		}


		ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		flagPkCreated = true;

		if (!flagPkCreated) {
			for(DBTableField field: restoreTable.getFields().values()) {
				if (field.getIsPrimaryKey()) {
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addPk"), 2);

					String ddl = MessageFormat.format(
							"ALTER TABLE {0} ADD CONSTRAINT PK_{1}_{2} PRIMARY KEY([{2}])",
							tblName, restoreTable.getTable().getName(), field.getName()
					);
					st.execute(ddl);

					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok")); break;
				}
			}
		}
	}

}
