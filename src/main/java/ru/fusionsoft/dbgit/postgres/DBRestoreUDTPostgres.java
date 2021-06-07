package ru.fusionsoft.dbgit.postgres;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.core.NotImplementedExceptionDBGitRuntime;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class DBRestoreUDTPostgres extends DBRestoreAdapter {
    @Override
    public final boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        throw new NotImplementedExceptionDBGitRuntime();
    }

    @Override
    public final void removeMetaObject(IMetaObject obj) throws Exception {
        throw new NotImplementedExceptionDBGitRuntime();
    }
}
