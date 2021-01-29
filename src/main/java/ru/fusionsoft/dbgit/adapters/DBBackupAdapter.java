package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class DBBackupAdapter implements IDBBackupAdapter {
	protected IDBAdapter adapter = null;
	
	private boolean toSaveData;
	private boolean saveToSchema;
	
	protected DBGitLang lang = DBGitLang.getInstance();
	protected DBGitIndex dbGitIndex;


	public void  setAdapter(IDBAdapter adapter) {
		this.adapter = adapter;
	}
	
	public IDBAdapter getAdapter() {
		return adapter;
	}

	public boolean isToSaveData() {
		return toSaveData;
	}

	public void setToSaveData(boolean toSaveData) {
		this.toSaveData = toSaveData;
	}
	
	public void saveToSchema(boolean saveToSchema) {
		this.saveToSchema = saveToSchema;
	}
	
	public boolean isSaveToSchema() {
		return saveToSchema;
	}


	public boolean isExists(IMetaObject imo) {
		NameMeta nm = new NameMeta(imo.getName());
		try {
			return isExists(nm.getSchema(), nm.getName());
		} catch (Exception ex){
			ex.printStackTrace();
			throw new RuntimeException(ex);
		}
	}

	public void backupDatabase(IMapMetaObject updateObjs) throws Exception {

		// Condition: we should not refer from backups on non-backup objects
		// ->  we need to backup restoring objects dependencies

		// Condition: old backups can refer on each other
		// ->  we need to drop old backups of the objects + old backup dependencies

		//	collect yet existing backups of restore objects
//		Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
		dbGitIndex = DBGitIndex.getInctance();

		StatementLogging stLog = new StatementLogging(adapter.getConnection(), adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		IMapMetaObject dbObjs = GitMetaDataManager.getInstance().loadDBMetaData(true);

		IMapMetaObject dbToBackup = new TreeMapMetaObject(dbObjs.values().stream()
			.filter( x-> updateObjs.containsKey(x.getName()) )
			.collect(Collectors.toList()));

		List<IMetaObject> dbNotToBackup = dbObjs.values().stream()
			.filter(x -> !updateObjs.containsKey(x.getName()))
			.collect(Collectors.toList());

		IMapMetaObject dbAllBackups = new TreeMapMetaObject(dbObjs.values().stream()
			.filter(this::isBackupObject)
			.collect(Collectors.toList()));

		ConsoleWriter.println(DBGitLang.getInstance()
		    .getValue("general", "backup", "tryToBackup")
		    .withParams(
				String.valueOf(dbToBackup.size()),
				String.valueOf(updateObjs.size()))
		    , 1
		);


		// collect restore objects dependencies to satisfy all backups create needs
		// so dependencies will be backed up too
		Map<String, IMetaObject> addedObjs;
		do{
			addedObjs = dbNotToBackup.stream().filter( notToBackup ->
				notToBackup.getUnderlyingDbObject() != null &&
				dbToBackup.values().stream().anyMatch(
					toBackup -> toBackup.dependsOn(notToBackup)
				)
			).collect(Collectors.toMap(IMetaObject::getName, Function.identity() ));

			dbNotToBackup.removeAll(addedObjs.values());
			dbToBackup.putAll(addedObjs);

			if(addedObjs.size() > 0) {
				ConsoleWriter.println(DBGitLang.getInstance()
					.getValue("general", "backup", "dependingBackups")
					.withParams(
						String.valueOf(addedObjs.size()),
						String.join(" ,", addedObjs.keySet())
					), 2
				);
			}
		} while (addedObjs.size() > 0);

		//so we have a full backup list, let's get a drop list
		if(dbToBackup.size() > 0){
			Set<String> suspectBackupNames = dbToBackup.values().stream().map( x->getBackupNameMeta(x).getMetaName()).collect(Collectors.toSet());
			List<IMetaObject> dropList = new ArrayList<>();

			IMapMetaObject dbDroppingBackups = new TreeMapMetaObject(dbAllBackups.values().stream()
				.filter(dbBackup -> suspectBackupNames.contains(dbBackup.getName()))
				.collect(Collectors.toList()));

			List<IMetaObject> dbDroppingBackupsDeps = dbAllBackups.values().stream()
				.filter( dbBackup ->
					!dbDroppingBackups.containsKey(dbBackup.getName())
					&& dbDroppingBackups.values().stream().anyMatch(dbBackup::dependsOn)
				).collect(Collectors.toList());

			dropList.addAll(dbDroppingBackups.values());
			dropList.addAll(dbDroppingBackupsDeps);
			List<IMetaObject> dropListSorted =  new SortedListMetaObject(dropList).sortFromDependencies();

			if(dropList.size() > 0) ConsoleWriter.println(DBGitLang.getInstance()
				.getValue("general", "backup", "rewritingBackups")
				.withParams(
					String.valueOf(dbDroppingBackups.size()),
					String.valueOf(dbDroppingBackupsDeps.size())
				), messageLevel-1
			);

			//drop backups in one place
			for(IMetaObject imo : dropListSorted){
				ConsoleWriter.detailsPrintln(lang.getValue("general", "backup", "droppingBackup").withParams(imo.getName()), messageLevel);
				dropIfExists(imo, stLog);

				ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			}

			if(dbToBackup.size() > 0) ConsoleWriter.println(DBGitLang.getInstance()
				.getValue("general", "backup", "creatingBackups")
				.withParams(
					String.valueOf(dbToBackup.size())
				), messageLevel-1
			);


			//create backups
			for(IMetaObject imo : dbToBackup.getSortedList().sortFromReferenced()){
				backupDBObject(imo);
			}

			//	Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
			//	ConsoleWriter.detailsPrintLn(MessageFormat.format("({0})",
			//			lang.getValue("general", "add", "ms").withParams(String.valueOf(timestampAfter.getTime() - timestampBefore.getTime()))
			//	));
		}

	}

	public NameMeta getBackupNameMeta(IMetaObject imo){
		NameMeta nm = new NameMeta(imo.getName());
		String backupName = isSaveToSchema() ? nm.getName() : PREFIX + nm.getName();
		String backupSchema = isSaveToSchema() ? PREFIX + nm.getSchema() : nm.getSchema();
		return new NameMeta(backupSchema, backupName, (DBGitMetaType) imo.getType());
	}

	public boolean isBackupObject(IMetaObject imo) {
		if(dbGitIndex != null && dbGitIndex.getTreeItems().containsKey(imo.getName())) return false;
		NameMeta nm = new NameMeta(imo);
		return nm.getName().contains(PREFIX) || nm.getSchema().contains(PREFIX);
	}
	
}
