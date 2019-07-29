package ru.fusionsoft.dbgit.postgres;

import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;

public class FactoryDBBackupAdapterPostgres implements IFactoryDBBackupAdapter {

	private IDBBackupAdapter backupAdapter = null;
	
	@Override
	public IDBBackupAdapter getBackupAdapter(IDBAdapter adapter) throws Exception {
		if (backupAdapter == null) {
			backupAdapter = new DBBackupAdapterPostgres();
			backupAdapter.setAdapter(adapter);
			backupAdapter.saveToSchema(DBGitConfig.getInstance().getBoolean("core", "BACKUP_TO_SCHEME", false));
			backupAdapter.setToSaveData(DBGitConfig.getInstance().getBoolean("core", "BACKUP_TABLEDATA", false));
		}

		return backupAdapter;
	}

}
