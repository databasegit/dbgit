package ru.fusionsoft.dbgit.postgres;

import com.diogonunes.jcdp.color.api.Ansi;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DBRestoreTablePostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if(Integer.valueOf(step).equals(0)) {
			restoreTablePostgres(obj);
			return false;
		}

		if(Integer.valueOf(step).equals(1)) {
			restoreTableIndexesPostgres(obj);
			return false;
		}

		if(Integer.valueOf(step).equals(-1)) {
			restoreTableConstraintPostgres(obj);
			return false;
		}

		return true;
	}

	public void restoreTablePostgres(IMetaObject obj) throws Exception
	{
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				MetaTable existingTable = new MetaTable(restoreTable.getTable());

				String schema = getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase());
				String tblName = DBAdapterPostgres.escapeNameIfNeeded(restoreTable.getTable().getName());

				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreTable").withParams(schema+"."+tblName), 1);

				//find existing table and set tablespace or create
				if(existingTable.loadFromDB()){
					String restoreTablespaceSam = MessageFormat.format(
						"alter table {0}.{1} set tablespace {2}"
						,schema
						,tblName
						,restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")
							? restoreTable.getTable().getOptions().get("tablespace").getData()
							: "pg_default"
					);
					st.execute(restoreTablespaceSam);
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				} else {
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "createTable"), 2);
					String createTableDdl = MessageFormat.format(
						"create table {0}.{1}() tablespace {2};\n alter table {0}.{1} owner to {3}"
						,schema
						,tblName
						,restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")
							? restoreTable.getTable().getOptions().get("tablespace").getData()
							: "pg_default"
						,restoreTable.getTable().getOptions().getChildren().containsKey("owner")
							? restoreTable.getTable().getOptions().get("owner").getData()
							: "postgres"
					);
					st.execute(createTableDdl);
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				}

				//restore comment
				if (restoreTable.getTable().getComment() != null && restoreTable.getTable().getComment().length() > 0){
					st.execute(MessageFormat.format(
						"COMMENT ON TABLE {0}.{1} IS '{2}'"
						,schema
						,tblName
						,restoreTable.getTable().getComment()
					));
				}
				
				//restore tabl fields
//				Map<String, DBTableField> currentFileds = adapter.getTableFields(schema.toLowerCase(), restoreTable.getTable().getName().toLowerCase());
				MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),existingTable.getFields());
				String tblSam = schema + "." + DBAdapterPostgres.escapeNameIfNeeded(tblName);

				if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addColumns"), 2);

					Comparator<DBTableField> comparator = Comparator.comparing(DBTableField::getOrder);

					List<DBTableField> values = new ArrayList<>(diffTableFields.entriesOnlyOnLeft().values());
					values.sort(comparator);

					for(DBTableField tblField : values) {
						String fieldName = DBAdapterPostgres.escapeNameIfNeeded(tblField.getName());
						st.execute(
						"alter table "+ tblSam +" add column "
							+ fieldName + " "
							+ tblField.getTypeSQL().replace("NOT NULL", "")
						);

						if (tblField.getDescription() != null && tblField.getDescription().length() > 0)
							st.execute(
							"COMMENT ON COLUMN " + tblSam + "."
								+ fieldName
								+ " IS '" + tblField.getDescription() + "'"
							);

						if (!tblField.getIsNullable()) {
							st.execute(
							"alter table " + tblSam
								+ " alter column " + fieldName
								+ " set not null"
							);
						}

						if (tblField.getDefaultValue() != null && tblField.getDefaultValue().length() > 0) {
							st.execute("alter table " + tblSam + " alter column " + fieldName
									+ " SET DEFAULT " + tblField.getDefaultValue());
						}

					}
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				}
							
				if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "droppingColumns"), 2);
					for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
						st.execute("alter table "+ tblSam +" drop column "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.getName()));
					}
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				}

				if(!diffTableFields.entriesDiffering().isEmpty()) {
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "modifyColumns"), 2);
					for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
						if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
							st.execute(
							"alter table "
								+ tblSam
								+" rename column "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.rightValue().getName())
								+" to "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName())
							);
						}

						if (restoreTable.getTable().getComment() != null && restoreTable.getTable().getComment().length() > 0)
							st.execute("COMMENT ON COLUMN " + tblSam + "." + DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName()) + " IS '" + tblField.leftValue().getDescription() + "'");

						if(	!tblField.leftValue().getTypeSQL().equals(tblField.rightValue().getTypeSQL())
							&& tblField.rightValue().getTypeUniversal() != FieldType.BOOLEAN) {
							st.execute(
							"alter table "
								+ tblSam
								+" alter column "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName())
								+" type "+ tblField.leftValue().getTypeSQL().replace("NOT NULL", "")
							);
							if (!tblField.leftValue().getIsNullable()) {
								st.execute(
								"alter table " + tblSam
									+ " alter column " + DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName())
									+ " set not null"
								);
							}
						}
					}
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				}

			}
			else {
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			connect.commit();
			st.close();
		}
	}
	public void restoreTableFieldsPostgres(IMetaObject obj) throws Exception
	{
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				String tblName = schema+"."+restoreTable.getTable().getName();
				Map<String, DBTable> tables = adapter.getTables(schema);
				boolean exist = false;
				if(!(tables == null || tables.isEmpty() )) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equals(table.getName())){
							exist = true;
							Map<String, DBTableField> currentFileds = adapter.getTableFields(schema, restoreTable.getTable().getName());
							MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFileds);

							if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
								for(DBTableField tblField:diffTableFields.entriesOnlyOnLeft().values()) {
									st.execute("alter table "+ tblName +" add column " + tblField.getName()  + " " + tblField.getTypeSQL());

									if (tblField.getDescription() != null && tblField.getDescription().length() > 0)
										st.execute("COMMENT ON COLUMN " + tblName + "." + ((adapter.isReservedWord(tblField.getName()) || tblField.getNameExactly()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " IS '" + tblField.getDescription() + "'");

									if (!tblField.getIsNullable()) {
										st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " set not null");
									}

									if (tblField.getDefaultValue() != null && tblField.getDefaultValue().length() > 0) {
										st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName())
												+ " SET DEFAULT " + tblField.getDefaultValue());
									}
								}
							}

							if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
								for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
									st.execute("alter table "+ tblName +" drop column IF EXISTS "+ tblField.getName());
								}
							}

							if(!diffTableFields.entriesDiffering().isEmpty()) {
								for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
									if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
										st.execute("alter table "+ tblName +" rename column "+ tblField.rightValue().getName() +" to "+ tblField.leftValue().getName());
									}

									if(!tblField.leftValue().getTypeSQL().equals(tblField.rightValue().getTypeSQL())) {
										st.execute("alter table "+ tblName +" alter column "+ tblField.leftValue().getName() +" type "+ tblField.leftValue().getTypeSQL());
									}
								}
							}
						}
					}
				}
				if(!exist){
					for(DBTableField tblField:restoreTable.getFields().values()) {
						st.execute("alter table "+ tblName +" add column " + tblField.getName()  + " " + tblField.getTypeSQL());

						if (tblField.getDescription() != null && tblField.getDescription().length() > 0)
							st.execute("COMMENT ON COLUMN " + tblName + "." + ((adapter.isReservedWord(tblField.getName()) || tblField.getNameExactly()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " IS '" + tblField.getDescription() + "'");

						if (!tblField.getIsNullable()) {
							st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " set not null");
						}

						if (tblField.getDefaultValue() != null && tblField.getDefaultValue().length() > 0) {
							st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName())
									+ " SET DEFAULT " + tblField.getDefaultValue());
						}

					}
				}

				ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraintscount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+tblName+"'::regclass");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraintscount"));
				if(constraintsCount.intValue()>0) {
					removeTableConstraintsPostgres(obj);
				}
				// set primary key
				for(DBConstraint tableconst: restoreTable.getConstraints().values()) {
					if(tableconst.getConstraintType().equals("p")) {
						st.execute("alter table "+ tblName +" add constraint "+ DBAdapterPostgres.escapeNameIfNeeded(tableconst.getName()) + " "+tableconst.getSql().replace(" " + tableconst.getSql() + ".", " " + schema + "."));
						break;
					}
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
	public void restoreTableIndexesPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreIndex").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				MetaTable existingTable = new MetaTable(restoreTable.getTable());
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());

				if(existingTable.loadFromDB()){
					MapDifference<String, DBIndex> diffInd = Maps.difference(restoreTable.getIndexes(), existingTable.getIndexes());

					for(DBIndex ind:diffInd.entriesOnlyOnLeft().values()) {
						if(restoreTable.getConstraints().containsKey(ind.getName())) {continue;}
						st.execute(MessageFormat.format("{0} {1}"
							,ind.getSql().replace(" INDEX ", " INDEX IF NOT EXISTS ")
							,ind.getOptions().getChildren().containsKey("tablespace") ? " tablespace " + ind.getOptions().get("tablespace").getData() : ""
						));
					}

					for(DBIndex ind:diffInd.entriesOnlyOnRight().values()) {
						if(existingTable.getConstraints().containsKey(ind.getName())) continue;
						st.execute("drop index if exists "+schema+"."+ DBAdapterPostgres.escapeNameIfNeeded(ind.getName()));
					}

					for(ValueDifference<DBIndex> ind : diffInd.entriesDiffering().values()) {
						DBIndex restoreIndex = ind.leftValue();
						DBIndex existingIndex = ind.rightValue();
						if(!restoreIndex.getSql().equalsIgnoreCase(existingIndex.getSql())) {
							st.execute(MessageFormat.format("drop index {0}.{1};\n{2};\n",
								schema
								, DBAdapterPostgres.escapeNameIfNeeded(existingIndex.getName())
								, restoreIndex.getSql()
							));
						}

						st.execute(MessageFormat.format(
							"alter index {0}.{1} set tablespace {2}"
							,schema
							,DBAdapterPostgres.escapeNameIfNeeded(existingIndex.getName())
							,restoreIndex.getOptions().getChildren().containsKey("tablespace")
								? restoreIndex.getOptions().get("tablespace").getData()
								: "pg_default"
						));
					}
					connect.commit();
				} else {
					// TODO error if not exists
				}
			}
			else {
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
	public void restoreTableConstraintPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreConstr").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				MetaTable existingTable = new MetaTable(restoreTable.getTable());
				existingTable.loadFromDB();

				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());

				MapDifference<String, DBConstraint> diff = Maps.difference(existingTable.getConstraints(), restoreTable.getConstraints());

				//drop unneeded
				for(DBConstraint constr : diff.entriesOnlyOnLeft().values()){
					st.execute(MessageFormat.format(
						"alter table {0}.{1} drop constraint {2};\n"
						, schema
						, DBAdapterPostgres.escapeNameIfNeeded(existingTable.getTable().getName())
						, DBAdapterPostgres.escapeNameIfNeeded(constr.getName())
					));
				}

				// restore not existing
				// not restore index if not exists and have the same named PK
				Set<String> typesFirst = Sets.newHashSet("p", "u");
				Comparator<DBConstraint> pksFirstComparator = Comparator.comparing(x->!typesFirst.contains(x.getConstraintType()));
				for(DBConstraint constr : diff.entriesOnlyOnRight().values().stream().sorted(pksFirstComparator).collect(Collectors.toList()) )  {
					createConstraint(restoreTable, constr, st, false);
				}

				//process intersects
				for (ValueDifference<DBConstraint> constr : diff.entriesDiffering().values()){
					MapDifference<String, StringProperties> propsDiff = Maps.difference(constr.leftValue().getOptions().getChildren(), constr.leftValue().getOptions().getChildren());
					ConsoleWriter.printlnColor("Difference in constraints: " + propsDiff.toString(), Ansi.FColor.MAGENTA, 1);
					createConstraint(restoreTable, constr.rightValue(), st, true);
				}

				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			else {
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

	private void createConstraint(MetaTable restoreTable, DBConstraint constr, StatementLogging st, boolean replaceExisting) throws Exception {
		String schema = getPhisicalSchema(constr.getSchema());
		String tableSam = schema + "." + DBAdapterPostgres.escapeNameIfNeeded(restoreTable.getTable().getName());
		String constrName = DBAdapterPostgres.escapeNameIfNeeded(constr.getName());
		String constrDdl = (replaceExisting) ? MessageFormat.format("alter table {0} drop constraint {1};\n", tableSam, constrName) : "";
		constrDdl += MessageFormat.format(
			"alter table {0} add constraint {1} {2};\n"
			,tableSam
			,constrName
			,constr.getSql()
				.replace(" " + constr.getSchema() + ".", " " + schema + ".")
				.replace("REFERENCES ", "REFERENCES " + schema + ".")
		);
		st.execute(constrDdl);
	}


	public void removeTableConstraintsPostgres(IMetaObject obj) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());

				Map<String, DBConstraint> constraints = table.getConstraints();
				for(DBConstraint constrs :constraints.values()) {
					st.execute(
					"alter table "+ schema +"."+DBAdapterPostgres.escapeNameIfNeeded(table.getTable().getName())
						+" drop constraint if exists "+DBAdapterPostgres.escapeNameIfNeeded(constrs.getName())
					);
				}
			}
			else {
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		}
	}

	public void removeTableIndexesPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());

				Map<String, DBIndex> constraints = table.getIndexes();
				for(DBIndex constrs :constraints.values()) {
					st.execute(MessageFormat.format(
						"alter table {0}.{1} drop index if exists {2}"
						,schema
						,DBAdapterPostgres.escapeNameIfNeeded(table.getTable().getName())
						,DBAdapterPostgres.escapeNameIfNeeded(constrs.getName())
					));
				}
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

	/*public void removeIndexesPostgres(IMetaObject obj) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				Map<String, DBIndex> indexes = table.getIndexes();
				for(DBIndex index :indexes.values()) {
					st.execute("DROP INDEX IF EXISTS "+index.getName());
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to remove TableIndexes.");
			}
		}
		catch(Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		}
	}*/

	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			MetaTable tblMeta = (MetaTable)obj;
			DBTable tbl = tblMeta.getTable();
			if (tbl == null) return;

			String schema = getPhisicalSchema(tbl.getSchema());

			freeTableSequences(tbl, connect, st);
			st.execute("DROP TABLE "+schema+"."+DBAdapterPostgres.escapeNameIfNeeded(tbl.getName()));
			connect.commit();
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

	private void freeTableSequences(DBTable tbl, Connection conn, StatementLogging st) throws SQLException {
		Statement stService = conn.createStatement();
		ResultSet rsSequences = stService.executeQuery("SELECT n.nspname as schema, s.relname as sequence, t.relname as table\n" +
				"	FROM pg_class s, pg_depend d, pg_class t, pg_attribute a, pg_namespace n \n" +
				"	WHERE s.relkind   = 'S' \n" +
				"	AND n.oid         = s.relnamespace \n" +
				"	AND d.objid       = s.oid \n" +
				"	AND d.refobjid    = t.oid \n" +
				"	AND (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum)\n" +
				"	AND n.nspname = '"+tbl.getSchema()+"' AND t.relname = '"+tbl.getName()+"'");

		while (rsSequences.next()) st.execute(MessageFormat.format(
			"ALTER SEQUENCE {0}.{1} OWNED BY NONE"
			, rsSequences.getString("schema")
			, rsSequences.getString("sequence"))
		);

		stService.close();
	}

}
