package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.data_table.*;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <div class="en">The base adapter adapter class. Contains general solutions independent of a particular database</div>
 * <div class="ru">Базовый класс адаптера БД. Содержит общие решения, независимые от конкретной БД</div>
 *
 * @author mikle
 *
 */
public abstract class DBAdapter implements IDBAdapter {
	protected Connection connect;
	protected Boolean isExec = true;
	protected OutputStream streamSql = null;
	protected DBGitLang lang = DBGitLang.getInstance();

	@Override
	public void setConnection(Connection conn) {
		connect = conn;
	}

	@Override
	public Connection getConnection() {
		try {
			int maxTriesCount = DBGitConfig.getInstance().getInteger("core", "TRY_COUNT", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000));
			int pauseTimeSeconds = DBGitConfig.getInstance().getInteger("core", "TRY_DELAY", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000));
			int currentTry = 0;

			if (connect.isValid(0))
				return connect;

			else {
				ConsoleWriter.println("Connection lost, trying to reconnect...");
				while (currentTry <= maxTriesCount) {
					TimeUnit.SECONDS.sleep(pauseTimeSeconds);
					currentTry++;
					ConsoleWriter.println("Try " + currentTry);
					DBConnection conn = DBConnection.getInctance(false);
					if (conn.testingConnection()) {
						conn.flushConnection();
						conn = DBConnection.getInctance(true);
						ConsoleWriter.println("Successful reconnect");
						connect = conn.getConnect();
						return connect;
					}
				}
				throw new ExceptionDBGit(lang.getValue("errors", "connectionError").toString());


			}
		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}

	@Override
	public void setDumpSqlCommand(OutputStream stream, Boolean isExec) {
		this.streamSql = stream;
		this.isExec = isExec;
	}

	@Override
	public OutputStream getStreamOutputSqlCommand() {
		return streamSql;
	}

	@Override
	public Boolean isExecSql() {
		return isExec;
	}

	@Override
	public void restoreDataBase(IMapMetaObject updateObjs) throws Exception {
		Connection connect = getConnection();
		DBGitLang lang = DBGitLang.getInstance();

		IMapMetaObject currStep = updateObjs;

		try {
			List<MetaTable> tables = new ArrayList<MetaTable>();
			List<MetaTableData> tablesData = new ArrayList<MetaTableData>();

			List<String> createdSchemas = new ArrayList<String>();
			List<String> createdRoles = new ArrayList<String>();
			SortedListMetaObject restoreObjs = updateObjs.getSortedList();
/*
			restoreObjs.sortFromFree().forEach( (x) -> {
				ConsoleWriter.detailsPrintlnGreen(MessageFormat.format(
					"{0}. {1} {2}"
					,restoreObjs.sortFromFree().indexOf(x)
					,x.getName()
					,(x.getUnderlyingDbObject() != null) && (x.getUnderlyingDbObject().getDependencies() != null) && (x.getUnderlyingDbObject().getDependencies().size() > 0)
						? "depends on (" + String.join(", ", x.getUnderlyingDbObject().getDependencies()) + ")"
						: ""
				));
			});
*/

			//if(toMakeBackup){
				//IDBBackupAdapter ba = getBackupAdapterFactory().getBackupAdapter(this);
				//ba.backupDatabase(updateObjs);
			//}


			for (IMetaObject obj : restoreObjs.sortFromFree()) {
				Integer step = 0;

				String schemaName = getSchemaName(obj);
				if (schemaName != null) {
					schemaName = (SchemaSynonym.getInstance().getSchema(schemaName) != null)
							? SchemaSynonym.getInstance().getSchema(schemaName)
							: schemaName;
				}

				boolean res = false;
				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());

				if (step == 0) {
					IDBConvertAdapter convertAdapter = getConvertAdapterFactory().getConvertAdapter(obj.getType().getValue());

					boolean isContainsNative = false;
					if (obj instanceof MetaTable) {
						MetaTable table = (MetaTable) obj;
						for (DBTableField field : table.getFields().values()) { if (field.getTypeUniversal().equals("native")) { isContainsNative = true; break; } }
					}

					if (isContainsNative) {
						ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "restore", "unsupportedTypes").withParams(obj.getName()));
						continue;
					}

					if (convertAdapter != null) {
						if (!createdSchemas.contains(schemaName) && schemaName != null) {
							createSchemaIfNeed(schemaName);
							createdSchemas.add(schemaName);
						}

						String ownerName = getOwnerName(obj);
						if (!getRoles().containsKey(ownerName) && !createdRoles.contains(ownerName) && ownerName != null) {
							createRoleIfNeed(ownerName);
							createdRoles.add(ownerName);
						}

						obj = convertAdapter.convert(getDbType(), getDbVersion(), obj);
					}

					if (
							step == 0
									&& DBGitConfig.getInstance().getBoolean("core", "TO_MAKE_BACKUP", true) && schemaName != null
									&& getBackupAdapterFactory().getBackupAdapter(this).isExists(schemaName, obj.getName().substring(obj.getName().indexOf("/") + 1, obj.getName().indexOf(".")))
					) {
						obj = getBackupAdapterFactory().getBackupAdapter(this).backupDBObject(obj);
					}
				}

				while (!res) {
					if (obj.getDbType() == null) {
						ConsoleWriter.println(lang.getValue("errors", "emptyDbType"));
						break;
					}

					if (getFactoryRestore().getAdapterRestore(obj.getType(), this) == null ||
							!obj.getDbType().equals(getDbType()))
						break;

					if (!createdSchemas.contains(schemaName) && schemaName != null) {
						createSchemaIfNeed(schemaName);
						createdSchemas.add(schemaName);
					}

					String ownerName = getOwnerName(obj);
					if (!getRoles().containsKey(ownerName) && !createdRoles.contains(ownerName) && ownerName != null) {
						createRoleIfNeed(ownerName);
						createdRoles.add(ownerName);
					}

					if (obj instanceof MetaTable) {
						MetaTable table = (MetaTable) obj;
						if (!tables.contains(table))
							tables.add(table);
					}

					if (obj instanceof MetaTableData) {
						MetaTableData tableData = (MetaTableData) obj;
						if (!tables.contains(tableData))
							tablesData.add(tableData);
					}

					//call restoreAdapter.restoreMetaObject with the next 'step' until it returns true
					res = getFactoryRestore().getAdapterRestore(obj.getType(), this).restoreMetaObject(obj, step);
					step++;

					if (step > 100) {
						throw new Exception(lang.getValue("errors", "restore", "restoreErrorDidNotReturnTrue").toString());
					}
				}
				Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
				Long diff = timestampAfter.getTime() - timestampBefore.getTime();
				ConsoleWriter.println("(" + diff + " " + lang.getValue("general", "add", "ms") +")");
			}

			for (MetaTable table : tables) {
				getFactoryRestore().getAdapterRestore(DBGitMetaType.DBGitTable, this).restoreMetaObject(table, -1);
			}
/*
			for (MetaTableData tableData : tablesData) {
				getFactoryRestore().getAdapterRestore(DBGitMetaType.DbGitTableData, this).restoreMetaObject(tableData, -2);
			}
*/
			connect.commit();
		} catch (Exception e) {
			connect.rollback();
			ConsoleWriter.detailsPrintlnRed(e.getLocalizedMessage());
			e.printStackTrace();
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "restoreError").toString(), e);
		} finally {
			//connect.setAutoCommit(false);
		}

	}

	@Override
	public void deleteDataBase(IMapMetaObject deleteObjs)  throws Exception {
		deleteDataBase(deleteObjs, false);
	}

	public void deleteDataBase(IMapMetaObject deleteObjs, boolean isDeleteFromIndex) throws Exception {
		Connection connect = getConnection();
		DBGitIndex index = DBGitIndex.getInctance();

		try {
			//start transaction
			boolean toMakeBackup = DBGitConfig.getInstance().getBoolean("core", "TO_MAKE_BACKUP", true);

			List<IMetaObject> deleteObjsSorted = deleteObjs.getSortedList().sortFromDependant();

			for (IMetaObject obj : deleteObjsSorted) {
				if (toMakeBackup) { obj = getBackupAdapterFactory().getBackupAdapter(this).backupDBObject(obj); }
				getFactoryRestore().getAdapterRestore(obj.getType(), this).removeMetaObject(obj);
				if(isDeleteFromIndex) index.removeItem(obj);
			}

			connect.commit();
		} catch (Exception e) {
			connect.rollback();
			throw new ExceptionDBGitRestore(DBGitLang.getInstance().getValue("errors", "restore", "removeError").withParams(e.getLocalizedMessage()), e);
		} finally {
			//connect.setAutoCommit(false);
		}
	}

	public String cleanString(String str) {
		String dt = str.replace("\r\n", "\n");
		while (dt.contains(" \n")) dt = dt.replace(" \n", "\n");
		dt = dt.replace("\t", "   ").trim();

		return dt;
	}

	public void rowToProperties(ResultSet rs, StringProperties properties) {
		try {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				if (rs.getString(i) == null) continue ;

				properties.addChild(rs.getMetaData().getColumnName(i).toLowerCase(), cleanString(rs.getString(i)));
			}
		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}

	private String getSchemaName(IMetaObject obj) {
		if (obj instanceof MetaSql)
			return ((MetaSql) obj).getSqlObject().getSchema();
		else if (obj instanceof MetaTable)
			return ((MetaTable) obj).getTable().getSchema();
		else if (obj instanceof MetaSequence)
			return ((MetaSequence) obj).getSequence().getSchema();
		else return null;
	}

	private String getOwnerName(IMetaObject obj) {
		if (obj instanceof MetaSql)
			return ((MetaSql) obj).getSqlObject().getOwner();
		else if (obj instanceof MetaTable)
			return ((MetaTable) obj).getTable().getOptions().get("owner").getData();
		else if (obj instanceof MetaSequence)
			return ((MetaSequence) obj).getSequence().getOptions().get("owner").getData();
		else return null;
	}

	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(FieldType.BINARY, MapFileData.class);
		FactoryCellData.regMappingTypes(FieldType.BOOLEAN, BooleanData.class);
		FactoryCellData.regMappingTypes(FieldType.DATE, DateData.class);
		FactoryCellData.regMappingTypes(FieldType.NATIVE, StringData.class);
		FactoryCellData.regMappingTypes(FieldType.NUMBER, LongData.class);
		FactoryCellData.regMappingTypes(FieldType.STRING, StringData.class);
		FactoryCellData.regMappingTypes(FieldType.TEXT, TextFileData.class);
	}
}
