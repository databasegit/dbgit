package ru.fusionsoft.dbgit.postgres;

import com.diogonunes.jcdp.color.api.Ansi;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.NameMeta;
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

		if(Integer.valueOf(step).equals(-2)) {
			removeTableConstraintsPostgres(obj);
			removeTableIndexesPostgres(obj);
			return false;
		}

		return true;
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

			freeTableSequences(tbl, connect, st);
			st.execute("DROP TABLE "+adapter.escapeNameIfNeeded(schema)+"."+adapter.escapeNameIfNeeded(tbl.getName()));
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

	public void restoreTablePostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				MetaTable existingTable = new MetaTable(restoreTable.getTable());

				String schema = adapter.escapeNameIfNeeded(getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase()));
				String tblName = adapter.escapeNameIfNeeded(restoreTable.getTable().getName());
				String tblSam = adapter.escapeNameIfNeeded(schema) + "." + adapter.escapeNameIfNeeded(tblName);

				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreTable").withParams(schema+"."+tblName), 1);

				//find existing table and set tablespace or create
				if (existingTable.loadFromDB()){

					restoreTableTablespace(st, restoreTable, existingTable);
					restoreTableOwner(st, restoreTable, existingTable);
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));

				} else {

					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "createTable"), 2);
					createTable(st, restoreTable);
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));

				}


				restoreTableFields(restoreTable, existingTable, st);
				restoreTableComment(restoreTable, existingTable, st);
				restoreTablePartition(restoreTable, st);

			}
			else {
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
								,ind.getSql()/*.replace(" INDEX ", " INDEX IF NOT EXISTS ")*/
								,ind.getOptions().getChildren().containsKey("tablespace") ? " tablespace " + ind.getOptions().get("tablespace").getData() : ""
						));
					}

					for(DBIndex ind:diffInd.entriesOnlyOnRight().values()) {
						if(existingTable.getConstraints().containsKey(ind.getName())) continue;
						st.execute(MessageFormat.format("drop index if exists {0}.{1}"
								, adapter.escapeNameIfNeeded(schema)
								, adapter.escapeNameIfNeeded(ind.getName())
						));
					}

					for(ValueDifference<DBIndex> ind : diffInd.entriesDiffering().values()) {
						DBIndex restoreIndex = ind.leftValue();
						DBIndex existingIndex = ind.rightValue();
						if(!restoreIndex.getSql().equalsIgnoreCase(existingIndex.getSql())) {
							st.execute(MessageFormat.format(
									"DROP INDEX {0}.{1} CASCADE;\n" + "{2};\n", //TODO discuss CASCADE
									adapter.escapeNameIfNeeded(schema)
									, adapter.escapeNameIfNeeded(existingIndex.getName())
									, restoreIndex.getSql() //drop and re-create using full DDL from .getSql()
							));
							st.execute(MessageFormat.format(
									"alter index {0}.{1} set tablespace {2}"
									,adapter.escapeNameIfNeeded(schema)
									,adapter.escapeNameIfNeeded(restoreIndex.getName())
									,restoreIndex.getOptions().getChildren().containsKey("tablespace")
											? restoreIndex.getOptions().get("tablespace").getData()
											: "pg_default"
							));
						}
					}
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
		try {
			if (obj instanceof MetaTable) {
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreTableConstraints").withParams(obj.getName()), 1);
				MetaTable restoreTable = (MetaTable)obj;
				MetaTable existingTable = new MetaTable(restoreTable.getTable());
				existingTable.loadFromDB();

				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());

				MapDifference<String, DBConstraint> diff = Maps.difference(existingTable.getConstraints(), restoreTable.getConstraints());

				//drop unneeded
				for(DBConstraint constr : diff.entriesOnlyOnLeft().values()){
					st.execute(MessageFormat.format(
							"alter table {0}.{1} drop constraint {2};\n"
							, adapter.escapeNameIfNeeded(schema)
							, adapter.escapeNameIfNeeded(existingTable.getTable().getName())
							, adapter.escapeNameIfNeeded(constr.getName())
					));
				}

				// restore not existing
				// * not restore index if not exists and have the same named PK
				Set<String> typesFirst = Sets.newHashSet("p", "u");
				Comparator<DBConstraint> pksFirstComparator = Comparator.comparing(x->!typesFirst.contains(x.getConstraintType()));
				for(DBConstraint constr : diff.entriesOnlyOnRight().values().stream().sorted(pksFirstComparator).collect(Collectors.toList()) )  {
					createConstraint(restoreTable, constr, st, false /* * */);
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
			st.close();
		}
	}
	private NameMeta getEscapedNameMeta(MetaTable table) throws ExceptionDBGit {
		NameMeta nm = new NameMeta();
		String schema = adapter.escapeNameIfNeeded(getPhisicalSchema(table.getTable().getSchema().toLowerCase()));
		String tblName = adapter.escapeNameIfNeeded(table.getTable().getName());

		nm.setSchema(schema);
		nm.setName(tblName);
		nm.setType(DBGitMetaType.DBGitTable);
		return nm;
	}


	private void restoreTableFields(MetaTable restoreTable, MetaTable existingTable, StatementLogging st) throws Exception {
		String lastField = "";
		try {
			NameMeta nme = getEscapedNameMeta(existingTable);
			String tblSam = nme.getSchema()+"."+nme.getName();

			MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),existingTable.getFields());

			if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addColumns"), 0);

				List<DBTableField> fields = diffTableFields.entriesOnlyOnLeft().values().stream()
					.sorted(Comparator.comparing(DBTableField::getOrder))
					.collect(Collectors.toList());

				for(DBTableField tblField : fields) {
					lastField = tblField.getName();
					addColumn(tblSam, tblField, st);
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}

			if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "droppingColumns"), 2);
				for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
					lastField = tblField.getName();
					dropColumn(tblSam, tblField, st);
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}

			if(!diffTableFields.entriesDiffering().isEmpty()) {
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "modifyColumns"), 2);
				for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
					lastField = tblField.leftValue().getName();

					DBTableField restoreField = tblField.leftValue();
					DBTableField existingField = tblField.rightValue();
					if(	!isSameTypeSql(tblField.leftValue(), tblField.rightValue()) ) {
						if( true
							&&(existingField.getTypeUniversal() != FieldType.BOOLEAN)
							&&(restoreField.getTypeUniversal() != FieldType.BOOLEAN)
							&&(existingField.getTypeUniversal() != FieldType.STRING_NATIVE)
							&&(restoreField.getTypeUniversal() != FieldType.STRING_NATIVE)
							&& hasNotTypeSql(tblField, "json")
							&& hasNotTypeSql(tblField, "text[]")
							&& hasNotTypeSql(tblField, "text")
						){  //Lots of exclusions when this don't work
							alterTypeColumn(tblSam, tblField, st);
						} else {
							dropColumn(tblSam, tblField.rightValue(), st);
							addColumn(tblSam, tblField.leftValue(), st);
						}
					}

					restoreTableFieldComment(tblSam, tblField, st);
					restoreTableFieldDefaultValue(tblSam, tblField, st);

					if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
						throw new Exception("just 'differing' columns, but differs in names, was: " + tblField.rightValue().getName() + ", to restore: " + tblField.leftValue().getName());
						//	st.execute(
						//	"alter table "
						//		+ tblSam
						//		+" rename column "+ adapter.escapeNameIfNeeded(tblField.rightValue().getName())
						//		+" to "+ adapter.escapeNameIfNeeded(tblField.leftValue().getName())
						//	);
					}
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(
				lang.getValue("errors", "restore", "objectRestoreError").withParams(restoreTable.getName()+"#"+lastField)
				+ "\n" + e.getMessage()
			);
		}
	}
	private void createTable(StatementLogging st, MetaTable restoreTable) throws SQLException, ExceptionDBGit {
		NameMeta nme = getEscapedNameMeta(restoreTable);

		String createTableDdl = MessageFormat.format(
			"create table {0}.{1}() {2};\n alter table {0}.{1} owner to {3}"
			,nme.getSchema()
			,nme.getName()
			,restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")
				? "tablespace " + restoreTable.getTable().getOptions().get("tablespace").getData()
				: ""
			,restoreTable.getTable().getOptions().getChildren().containsKey("owner")
				? restoreTable.getTable().getOptions().get("owner").getData()
				: "postgres"

		);

		if (restoreTable.getTable().getOptions().getChildren().containsKey("partkeydef")) {
			createTableDdl = createTableDdl.replace(" ) ", ") PARTITION BY " +
					restoreTable.getTable().getOptions().getChildren().get("partkeydef")
			+ " ");
		}

		st.execute(createTableDdl);
	}
	private void restoreTableOwner(StatementLogging st, MetaTable restoreTable, MetaTable existingTable) throws SQLException, ExceptionDBGit {
		String schema = adapter.escapeNameIfNeeded(getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase()));
		String tblName = adapter.escapeNameIfNeeded(restoreTable.getTable().getName());

		StringProperties exOwner= existingTable.getTable().getOptions().get("owner");
		StringProperties restoreOwner = restoreTable.getTable().getOptions().get("owner");
		if(restoreOwner != null && ( exOwner == null || !exOwner.getData().equals(restoreOwner.getData()) ) ){
			String alterTableDdl = MessageFormat.format(
				"alter table {0}.{1} owner to {2};\n"
				,schema
				,tblName
				,restoreTable.getTable().getOptions().get("owner").getData()
			);
			st.execute(alterTableDdl);
		}
	}
	private void restoreTableTablespace(StatementLogging st, MetaTable restoreTable, MetaTable existingTable) throws SQLException, ExceptionDBGit {
		NameMeta nme = getEscapedNameMeta(existingTable);

		StringProperties exTablespace = existingTable.getTable().getOptions().get("tablespace");
		StringProperties restoreTablespace = restoreTable.getTable().getOptions().get("tablespace");
		if(restoreTablespace != null &&
			( exTablespace == null || !exTablespace.getData().equals(restoreTablespace.getData()) ) ){
			//TODO For now in postgres context tablespace is always missing!
			String alterTableDdl = MessageFormat.format(
				"alter table {0}.{1} set tablespace {2};\n"
				,nme.getSchema()
				,nme.getName()
				,restoreTable.getTable().getOptions().get("tablespace").getData()
			);
			st.execute(alterTableDdl);
		}
	}


	//TODO removeMetaObject
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


	//TODO restoreTablePostgres
	private void restoreTableComment(MetaTable restoreTable, MetaTable existingTable, Statement st) throws ExceptionDBGit, SQLException
	{

		String restoreTableComment = restoreTable.getTable().getComment();
		String existingTableComment = existingTable.getTable().getComment();
		boolean restoreCommentPresent = restoreTableComment != null && restoreTableComment.length() > 0;
		boolean existingCommentPresent =  existingTableComment != null && existingTableComment.length() > 0;

		if (restoreCommentPresent){
			boolean commentsDiffer = !existingCommentPresent || !existingTableComment.equals(restoreTableComment);
			if(commentsDiffer){
				st.execute(MessageFormat.format(
						"COMMENT ON TABLE {0}.{1} IS ''{2}''"
						,adapter.escapeNameIfNeeded(getPhisicalSchema(restoreTable.getTable().getSchema()))
						,adapter.escapeNameIfNeeded(restoreTable.getTable().getName())
						,restoreTableComment
				));
			}
		}
	}
	private void restoreTablePartition(MetaTable restoreTable, Statement st) throws ExceptionDBGit, SQLException
	{
		NameMeta nme = getEscapedNameMeta(restoreTable);
		StringProperties parent = restoreTable.getTable().getOptions().getChildren().get("parent");
		StringProperties pg_get_expr = restoreTable.getTable().getOptions().getChildren().get("pg_get_expr");

		if(parent != null && pg_get_expr != null){
			st.execute(MessageFormat.format(
					"ALTER TABLE {0}.{1} ATTACH PARTITION {0}.{2} {3}"
					, nme.getSchema()
					, parent
					, nme.getName()
					, pg_get_expr
			));
		}

	}


	//TODO restoreTableConstraintPostgres
	private void createConstraint(MetaTable restoreTable, DBConstraint constr, StatementLogging st, boolean replaceExisting) throws Exception {
		NameMeta nme = getEscapedNameMeta(restoreTable);
		String tblSam = nme.getSchema()+"."+nme.getName();
		String constrName = adapter.escapeNameIfNeeded(constr.getName());
		String constrDdl = (replaceExisting) ? MessageFormat.format("alter table {0} drop constraint {1};\n", tblSam, constrName) : "";
		constrDdl += MessageFormat.format(
			"alter table {0} add constraint {1} {2};\n"
			,tblSam
			,constrName
			,constr.getSql()
				.replace(" " + constr.getSchema() + ".", " " + nme.getSchema() + ".")
				.replace("REFERENCES ", "REFERENCES " +  nme.getSchema() + ".")
		);

		//dont create constraint on table with 'parent' key
		if (!restoreTable.getTable().getOptions().getChildren().containsKey("parent"))
			st.execute(constrDdl);
	}


	//TODO  restoreTableFields
	private void addColumn(String tblSam, DBTableField tblField, Statement st ) throws SQLException {
		String fieldName = adapter.escapeNameIfNeeded(tblField.getName());
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
			st.execute(
					"alter table " + tblSam + " alter column " + fieldName
							+ " SET DEFAULT " + tblField.getDefaultValue()
			);
		}

	}
	private void dropColumn(String tblSam, DBTableField tblField, Statement st) throws SQLException {
		st.execute("alter table "+ tblSam +" drop column "+ adapter.escapeNameIfNeeded(tblField.getName()));
	}
	private boolean isSameTypeSql(DBTableField left, DBTableField right){
		return left.getTypeSQL().equals(right.getTypeSQL());
	}

	private boolean hasNotTypeSql(ValueDifference<DBTableField> field, String typeSql){
		return  !field.leftValue().getTypeSQL().contains(typeSql) &&
				!field.rightValue().getTypeSQL().contains(typeSql);
	}
	private void alterTypeColumn(String tblSam, ValueDifference<DBTableField> tblField, Statement st) throws SQLException
	{
		st.execute(MessageFormat.format("ALTER TABLE {0} ALTER COLUMN {1} TYPE {2} USING ({3}::{4})"
				, tblSam
				, adapter.escapeNameIfNeeded(tblField.leftValue().getName())
				, tblField.leftValue().getTypeSQL().replace("NOT NULL", "")
				, adapter.escapeNameIfNeeded(tblField.leftValue().getName())
				, tblField.leftValue().getTypeSQL().replace("NOT NULL", "")
		));

		if (!tblField.leftValue().getIsNullable()) {
			st.execute(MessageFormat.format("ALTER TABLE {0} ALTER COLUMN {1} SET NOT NULL"
					, tblSam
					, adapter.escapeNameIfNeeded(tblField.leftValue().getName())
			));
		}
	}
	private void restoreTableFieldComment(String tableSam, ValueDifference<DBTableField> tblField, Statement st) throws SQLException
	{

		String restoreDesc = tblField.leftValue().getDescription();
		String existingDesc = tblField.rightValue().getDescription();
		boolean restoreDescPresent = restoreDesc != null && restoreDesc.length() > 0;

		if (restoreDescPresent){
			boolean existingDescPresent = existingDesc != null && existingDesc.length() > 0;
			boolean needsUpdate = !existingDescPresent || !existingDesc.equals(restoreDesc);
			if(needsUpdate){
				st.execute(MessageFormat.format(
						"COMMENT ON COLUMN {0}.{1} IS ''{2}''"
						,tableSam
						,adapter.escapeNameIfNeeded(tblField.leftValue().getName())
						,restoreDesc
				));
			}
		}
	}
	private void restoreTableFieldDefaultValue(String tableSam, ValueDifference<DBTableField> tblField, Statement st) throws ExceptionDBGit, SQLException
	{

		String restoreDefault = tblField.leftValue().getDefaultValue();
		String existingDefault = tblField.rightValue().getDefaultValue();
		boolean restoreDefaultPresent = restoreDefault != null && restoreDefault.length() > 0;

		if (restoreDefaultPresent){
			boolean existingDescPresent = existingDefault != null && existingDefault.length() > 0;
			boolean needsUpdate = !existingDescPresent || !existingDefault.equals(restoreDefault);
			if(needsUpdate){
				st.execute(MessageFormat.format(
						"ALTER TABLE {0} ALTER COLUMN {1} SET DEFAULT {2}"
						,tableSam
						,adapter.escapeNameIfNeeded(tblField.leftValue().getName())
						,restoreDefault
				));
			}
		}
	}


	//TODO unused
	private void removeTableIndexesPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());


				Map<String, DBIndex> indices = table.getIndexes();
				for(DBIndex index :indices.values()) {
					if(table.getConstraints().containsKey(index.getName())) { continue; }

					st.execute(MessageFormat.format(
						"DROP INDEX IF EXISTS {0}.{1}"
						,adapter.escapeNameIfNeeded(schema)
//						,adapter.escapeNameIfNeeded(table.getTable().getName())
						,adapter.escapeNameIfNeeded(index.getName())
					));
				}
			}
			else {
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		}
	}
	private void removeTableConstraintsPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());
				MetaTable dbTable = (MetaTable) GitMetaDataManager.getInstance().getCacheDBMetaObject(obj.getName());


				Map<String, DBConstraint> constraints = dbTable.getConstraints();
				for(DBConstraint constrs :constraints.values()) {
					st.execute(
					"alter table "+ adapter.escapeNameIfNeeded(schema) +"."+adapter.escapeNameIfNeeded(table.getTable().getName())
						+" drop constraint if exists "+adapter.escapeNameIfNeeded(constrs.getName())
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
}
