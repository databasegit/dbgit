package ru.fusionsoft.dbgit.adapters;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;
import com.diogonunes.jcdp.color.api.Ansi;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.data_table.*;
import ru.fusionsoft.dbgit.dbobjects.DBOptionsObject;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
			int pauseTimeSeconds = DBGitConfig.getInstance().getInteger("core", "TRY_DELAY", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 2));
			int currentTry = 0;

			if (connect.isValid(0)){
				connect.setAutoCommit(false);
				return connect;
			} else {
				ConsoleWriter.println(DBGitLang.getInstance()
				    .getValue("general", "dbAdapter", "connectionLost")
				    , messageLevel-1
				);
				while (currentTry <= maxTriesCount) {
					TimeUnit.SECONDS.sleep(pauseTimeSeconds);
					currentTry++;
					ConsoleWriter.println(DBGitLang.getInstance()
						.getValue("general", "dbAdapter", "reconnectTry")
						.withParams(String.valueOf(currentTry))
						, messageLevel
					);
					DBConnection conn = DBConnection.getInstance(false);
					if (conn.testingConnection()) {
//						conn.flushConnection();
						conn = DBConnection.getInstance(true);
						ConsoleWriter.println(DBGitLang.getInstance()
							.getValue("general", "dbAdapter", "reconnectTrySuccess")
							, messageLevel
						);
						connect = conn.getConnect();
						connect.setAutoCommit(false);
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

		try {
			SortedListMetaObject tables = new SortedListMetaObject(updateObjs.values().stream().filter(x->x instanceof MetaTable ).collect(Collectors.toList()));
			SortedListMetaObject tablesExists = new SortedListMetaObject(updateObjs.values().stream().filter(x->x instanceof MetaTable && isExists(x)).collect(Collectors.toList()));

			Set<String> createdSchemas = getSchemes().values().stream().map(DBOptionsObject::getName).collect(Collectors.toSet());
			Set<String> createdRoles = getRoles().values().stream().map(DBRole::getName).collect(Collectors.toSet());

			// remove table indexes and constraints, which is step(-2) of restoreMetaObject(MetaTable)
			ConsoleWriter.println(lang.getValue("general", "restore", "droppingTablesConstraints"), messageLevel);
			for (IMetaObject table : tablesExists.sortFromDependencies()) {
				ConsoleWriter.println(lang.getValue("general", "restore", "droppingTableConstraints").withParams(table.getName()), messageLevel+1);
				getFactoryRestore().getAdapterRestore(DBGitMetaType.DBGitTable, this).restoreMetaObject(table, -2);
			}

			for (IMetaObject obj : updateObjs.getSortedList().sortFromReferenced()) {
				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
				int step = 0;
				boolean res = false;

				IDBAdapterRestoreMetaData restoreAdapter = getFactoryRestore().getAdapterRestore(obj.getType(), this) ;
				if(restoreAdapter == null) throw new Exception("restore adapter is null");
//				ConsoleWriter.printlnGreen(lang.getValue("general", "restore", "restoreType").withParams(obj.getType().toString().substring(5), obj.getName()));

				obj = tryConvert(obj);
				createRoleIfNeed(obj, createdRoles);
				createSchemaIfNeed(obj, createdSchemas);

				while (!res) {
					if(step==0) printRestoreMessage(obj);
					//if(step!=0) ConsoleWriter.print(" (step " + step + ")");
					res = restoreAdapter.restoreMetaObject(obj, step++);

					if (step > 100) { throw new Exception(lang.getValue("errors", "restore", "restoreErrorDidNotReturnTrue").toString()); }
				}

    			Long timeDiff = new Timestamp(System.currentTimeMillis()).getTime() - timestampBefore.getTime();
//				ConsoleWriter.detailsPrintColor(MessageFormat.format(" ({1} {2})"
//					, obj.getName()
//					, timeDiff
//					, lang.getValue("general", "add", "ms")), 0, Ansi.FColor.CYAN
//				);
			}

			// restore table constraints, which is step(-1) of restoreMetaObject(MetaTable)
			ConsoleWriter.println(lang.getValue("general", "restore", "restoringTablesConstraints"), messageLevel);
			for (IMetaObject table : tables.sortFromReferenced()) {
				getFactoryRestore().getAdapterRestore(DBGitMetaType.DBGitTable, this).restoreMetaObject(table, -1);
			}
			connect.commit();
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "restoreError").toString(), e);
		} finally {
		}
		
	}

	private void printRestoreMessage(IMetaObject obj) {
		String leafName = "";
		if(obj instanceof MetaSequence){ leafName = "restoreSeq"; }
		if(obj instanceof MetaView){ leafName = "restoreView"; }
		if(obj instanceof MetaTrigger){ leafName = "restoreTrigger"; }
		if(obj instanceof MetaSchema){ leafName = "restoreSchema"; }
		if(obj instanceof MetaUDT){ leafName = "restoreUDT"; }
		if(obj instanceof MetaEnum){ leafName = "restoreEnum"; }
		if(obj instanceof MetaDomain){ leafName = "restoreDomain"; }
		if(obj instanceof MetaRole){ leafName = "restoreRole"; }
		if(obj instanceof MetaProcedure){ leafName = "restorePrc"; }
		if(obj instanceof MetaPackage){ leafName = "restorePkg"; }
		if(obj instanceof MetaUser){ leafName = "restoreUser"; }
		if(obj instanceof MetaFunction){ leafName = "restoreFnc"; }
		if(obj instanceof MetaTable){ leafName = "restoreTable"; }
		if(obj instanceof MetaTableSpace){ leafName = "restoreTablespace"; }
		if(obj instanceof MetaTableData){ leafName = "restoreTableData"; }
		ConsoleWriter.println(lang.getValue("general", "restore", leafName).withParams(obj.getName()), 2);
	}

	@Override
	public void deleteDataBase(IMapMetaObject deleteObjs)  throws Exception {
		deleteDataBase(deleteObjs, false);
	}
	public void deleteDataBase(IMapMetaObject deleteObjs, boolean isDeleteFromIndex) throws Exception {
		Connection connect = getConnection();
		DBGitIndex index = DBGitIndex.getInctance();

		try {
			List<IMetaObject> deleteObjsSorted = deleteObjs.getSortedList().sortFromDependencies();
			for (IMetaObject obj : deleteObjsSorted) {
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

	private IMetaObject tryConvert(IMetaObject obj) throws Exception {
		if ( obj.getDbType() == null) throw new Exception(lang.getValue("errors", "emptyDbType").toString());

		if (isSameDbType(obj) ){
			if(isSameDbVersion(obj) || obj.getDbVersionNumber() <= getDbVersionNumber()){
				return obj;
			}
		} else {
			if ( checkContainsNativeFields(obj)) {
				ConsoleWriter.println(DBGitLang.getInstance()
				    .getValue("general", "restore", "unsupportedTypes")
				    .withParams(obj.getName())
				    , messageLevel
				);
			}
		}

		IDBConvertAdapter convertAdapter = getConvertAdapterFactory().getConvertAdapter(obj.getType().getValue());
		if (convertAdapter != null) return convertAdapter.convert(getDbType(), getDbVersion(), obj);
		else {
			String msg = DBGitLang.getInstance().getValue("errors", "convert", "cannotFindAdapter").withParams(
				obj.getName(), obj.getDbType().toString(), obj.getDbVersion()
				, getDbType().toString()
				, String.valueOf(getDbVersionNumber())
			);
			throw new Exception(msg);
		}
	}
	private void createSchemaIfNeed(IMetaObject obj, Set<String> createdSchemas) throws Exception {
		String schemaName = getSchemaSynonymName(obj);
		if(schemaName == null){
			ConsoleWriter.detailsPrintlnRed(DBGitLang.getInstance()
			    .getValue("errors", "adapter", "nullSchema")
			    .withParams(obj.getName())
			    , messageLevel
			);
			return;
		}
		if (!createdSchemas.contains(schemaName)) {
			createSchemaIfNeed(schemaName);
			createdSchemas.add(schemaName);
		}
	}
	private void createRoleIfNeed(IMetaObject obj, Set<String> createdRoles) throws ExceptionDBGit {
//		boolean isRolesUnignored = !DBGitIgnore.getInstance().matchOne("*." + DBGitMetaType.DBGitRole.getValue());
		String ownerName = getOwnerName(obj);

		if(ownerName != null){
			if ( !createdRoles.contains(ownerName)) {
				createRoleIfNeed(ownerName);
				createdRoles.add(ownerName);
			}
		}

	}
	protected boolean isExists(IMetaObject obj){
		try{
			IDBBackupAdapter backupAdapter = getBackupAdapterFactory().getBackupAdapter(AdapterFactory.createAdapter());
			return backupAdapter.isExists(
				obj.getUnderlyingDbObject().getSchema(),
				obj.getUnderlyingDbObject().getName()
			);
		} catch (Exception ex) {
			return false;
		}
	}

	private boolean checkContainsNativeFields(IMetaObject obj){
		if (obj instanceof MetaTable) {
			MetaTable table = (MetaTable) obj;
			for (DBTableField field : table.getFields().values()) {
				if (field.getTypeUniversal().equals(FieldType.NATIVE)) {
					return true;
				}
			}
		}
		return false;
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
	private String getSchemaName(IMetaObject obj) {
		if (obj instanceof MetaSql)
			return ((MetaSql) obj).getSqlObject().getSchema();
		else if (obj instanceof MetaTable)
			return ((MetaTable) obj).getTable().getSchema();
		else if (obj instanceof MetaSequence)
			return ((MetaSequence) obj).getSequence().getSchema();
		else if (obj instanceof MetaTableData)
			return ((MetaTableData) obj).getTable().getSchema();
		else return null;
	}
	private String getSchemaSynonymName(String schemaName) throws Exception {
		String schemaSynonym = schemaName != null
			? SchemaSynonym.getInstance().getSchema(schemaName)
			: null;

		return (schemaSynonym != null)
			? schemaSynonym
			: schemaName;
	}
	private String getSchemaSynonymName(IMetaObject obj) throws Exception {
		return getSchemaSynonymName(getSchemaName(obj));
	}
	public Double getDbVersionNumber(){
		Matcher matcher = Pattern.compile("\\D*(\\d+)\\.(\\d+)").matcher(getDbVersion());
		matcher.find();
		Double result = Double.valueOf(matcher.group(0)+matcher.group(1));
		return result;
	}
	private boolean isSameDbType(IMetaObject obj){
		return obj.getDbType().equals(getDbType());
	}
	private boolean isSameDbVersion(IMetaObject obj){
		return obj.getDbVersion().equals(getDbVersion());
	}

	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(FieldType.BINARY, MapFileData.class);
		FactoryCellData.regMappingTypes(FieldType.BOOLEAN, BooleanData.class);
		FactoryCellData.regMappingTypes(FieldType.DATE, DateData.class);
		FactoryCellData.regMappingTypes(FieldType.NATIVE, StringData.class);
		FactoryCellData.regMappingTypes(FieldType.NUMBER, LongData.class);
		FactoryCellData.regMappingTypes(FieldType.STRING, StringData.class);
		FactoryCellData.regMappingTypes(FieldType.STRING_NATIVE, StringData.class);
		FactoryCellData.regMappingTypes(FieldType.TEXT, TextFileData.class);
	}

	public String escapeNameIfNeeded(String name) {
		return name;
	}

	public NamedParameterPreparedStatement preparedStatement(Connection connection, String sql, Map<String, Object> values) throws SQLException, ExceptionDBGit {
		NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(getConnection(), sql);

		for(Map.Entry<String, Object> entry : values.entrySet()){
			Object value = entry.getValue();
			if (value instanceof String) stmt.setString(entry.getKey(), (String) value);
			else if (value instanceof Long) stmt.setLong(entry.getKey(), (Long) value);
			else if (value instanceof Float) stmt.setDouble(entry.getKey(), ((Float) value).floatValue());
			else throw new ExceptionDBGit("Unsupported type of value, see stacktrace");
		}

		return stmt;
	}
}
