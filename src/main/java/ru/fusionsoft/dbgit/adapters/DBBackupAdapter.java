package ru.fusionsoft.dbgit.adapters;

import com.diogonunes.jcdp.color.api.Ansi;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class DBBackupAdapter implements IDBBackupAdapter {
	protected IDBAdapter adapter = null;
	
	private boolean toSaveData;
	private boolean saveToSchema;
	
	protected DBGitLang lang = DBGitLang.getInstance();

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

	public void backupDatabase(IMapMetaObject backupObjs) throws Exception {

		// Condition: we should not refer from backups on non-backup objects
		// ->  we need to backup restoring objects dependencies

		// Condition: old backups can refer on each other
		// ->  we need to drop old backups of the objects + old backup dependencies

		//	collect yet existing backups of restore objects
		Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
		StatementLogging stLog = new StatementLogging(adapter.getConnection(), adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		IMapMetaObject dbObjs = GitMetaDataManager.getInstance().loadDBMetaData(true);

		IMapMetaObject backupList = new TreeMapMetaObject(dbObjs.values().stream()
			.filter( x-> backupObjs.containsKey(x.getName()) )
			.collect(Collectors.toList()));

		List<IMetaObject> nonBackupList = dbObjs.values().stream()
			.filter(x -> !backupObjs.containsKey(x.getName()))
			.collect(Collectors.toList());

		IMapMetaObject existingBackups = new TreeMapMetaObject(dbObjs.values().stream()
			.filter(this::isBackupObject)
			.collect(Collectors.toList()));



		ConsoleWriter.printlnColor(MessageFormat.format("Backing up {0} objects... {1} are in database:\n\t- {2}",
			backupObjs.size(), backupList.size(),
			String.join("\n\t- ", backupList.keySet())
		), Ansi.FColor.MAGENTA, 1);


		// collect restore objects dependencies to satisfy all backups create needs
		// so dependencies will be backed up too
		Map<String, IMetaObject> addedObjs;
		do{
			addedObjs = nonBackupList.stream().filter( nonBackup ->
				nonBackup.getUnderlyingDbObject() != null
				&& backupList.values().stream().anyMatch(
					backup -> backup.getUnderlyingDbObject().getDependencies().contains(nonBackup.getName())
				)
			).collect(Collectors.toMap(IMetaObject::getName, Function.identity() ));

			nonBackupList.removeAll(addedObjs.values());
			backupList.putAll(addedObjs);

			if(addedObjs.size() > 0) {
				ConsoleWriter.printlnColor(
					MessageFormat.format("- found {0} depending: {1}", addedObjs.size(), String.join(" ,", addedObjs.keySet())),
					Ansi.FColor.MAGENTA, 2
				);
			}
		} while (addedObjs.size() > 0);

		//so we have a full backup list, let's get a drop list

		if(backupList.size() > 0){

			IMapMetaObject backupListExisting = new TreeMapMetaObject(existingBackups.values().stream()
				.filter(x -> backupList.containsKey(x.getName()))
				.collect(Collectors.toList()));

			List<IMetaObject> backupListExistingDependant = existingBackups.values().stream()
				.filter( x ->
					!backupListExisting.containsKey(x.getName())
					|| x.getUnderlyingDbObject().getDependencies().contains(x.getName())
				)
				.collect(Collectors.toList());

			List<IMetaObject> dropList = new ArrayList<>(backupListExisting.values());
			dropList.addAll(backupListExistingDependant);
			List<IMetaObject> dropListSorted =  new SortedListMetaObject(dropList).sortFromDependant();

			ConsoleWriter.printlnColor("Rewriting "+dropList.size()+" backups (with dependencies)", Ansi.FColor.MAGENTA, 1);
			dropListSorted.forEach( x -> ConsoleWriter.printlnColor( "- " + x.getName(), Ansi.FColor.MAGENTA, 1));

			//drop backups in one place
			for(IMetaObject imo : dropListSorted){
				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "droppingBackup").withParams(imo.getName()), 2);

				dropIfExists(imo, stLog);

				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}

			Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
			ConsoleWriter.detailsPrintLn(MessageFormat.format("({0})",
					lang.getValue("general", "add", "ms").withParams(String.valueOf(timestampAfter.getTime() - timestampBefore.getTime()))
			));

			//create backups
			for(IMetaObject imo : backupList.getSortedList().sortFromFree()){
				backupDBObject(imo);
			}
		}

	}

	public NameMeta getBackupNameMeta(IMetaObject imo){
		NameMeta nm = new NameMeta(imo.getName());
		String backupName = isSaveToSchema() ? nm.getName() : PREFIX + nm.getName();
		String backupSchema = isSaveToSchema() ? PREFIX + nm.getSchema() : nm.getSchema();
		return new NameMeta(backupSchema, backupName, (DBGitMetaType) imo.getType());
	}

	public boolean isBackupObject(IMetaObject imo){
		NameMeta nm = new NameMeta(imo);
		return nm.getName().contains(PREFIX) || nm.getSchema().contains(PREFIX);
	}
	
}
