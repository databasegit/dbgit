package ru.fusionsoft.dbgit.adapters;

import com.diogonunes.jcdp.color.api.Ansi;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.util.List;
import java.util.Map;
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
		StatementLogging stLog = new StatementLogging(adapter.getConnection(), adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		//	collect backup objs THAT EXIST and related to them database objects THAT EXIST BY DEFINITION
		//	to recreate their backups
		IMapMetaObject fullBackupObjs = new TreeMapMetaObject(backupObjs.values().stream().filter(this::isExists).collect(Collectors.toList()));
		ConsoleWriter.printlnColor("Backing up " + backupObjs.size() + " objects... " + fullBackupObjs.size() + " are in database", Ansi.FColor.MAGENTA, 1);

		IMapMetaObject dbObjs = GitMetaDataManager.getInctance().loadDBMetaData();
		List<IMetaObject> nonBackupObjs = dbObjs.values().stream().filter(x -> !backupObjs.containsKey(x.getName())).collect(Collectors.toList());

		// do while none of nonBackupObjs depend on any of fullBackupObjs
		Map<String, IMetaObject> addedObjs;
		do{
			addedObjs = nonBackupObjs.stream().filter( x ->
				x.getUnderlyingDbObject() != null &&
				x.getUnderlyingDbObject().getDependencies().stream().anyMatch(fullBackupObjs::containsKey)
			).collect(Collectors.toMap(IMetaObject::getName, imo->imo ));

			nonBackupObjs.removeAll(addedObjs.values());
			fullBackupObjs.putAll(addedObjs);
			ConsoleWriter.printlnColor("- found "+addedObjs.keySet().size()+" depending: " + String.join(" ,", addedObjs.keySet()), Ansi.FColor.MAGENTA, 2);

		} while (addedObjs.size() > 0);
		SortedListMetaObject fullBackupSorted = fullBackupObjs.getSortedList();
		ConsoleWriter.printlnColor("Rewriting "+fullBackupObjs.size()+" backups (with dependencies)", Ansi.FColor.MAGENTA, 1);


		//drop backups
		for(IMetaObject imo : fullBackupSorted.sortFromDependant()){
			NameMeta backupNm = getBackupNameMeta(imo);
			IMetaObject backupImo = IMetaObject.create(backupNm.getMetaName());
			ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "droppingBackup").withParams(imo.getName()), 2);
			dropIfExists(backupImo, stLog);
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		}

		//create backups
		for(IMetaObject imo : fullBackupSorted.sortFromFree()){
			backupDBObject(imo);
		}

	}

	public NameMeta getBackupNameMeta(IMetaObject imo){
		NameMeta nm = new NameMeta(imo.getName());
		String backupName = isSaveToSchema() ? nm.getName() : PREFIX + nm.getName();
		String backupSchema = isSaveToSchema() ? PREFIX + nm.getSchema() : nm.getSchema();
		return new NameMeta(backupSchema, backupName, (DBGitMetaType) imo.getType());
	}
	
}
