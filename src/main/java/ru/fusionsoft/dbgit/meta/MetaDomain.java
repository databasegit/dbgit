package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBDomain;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;

public class MetaDomain extends MetaSql {
    public MetaDomain() {
    }

    public MetaDomain(DBSQLObject sqlObject) throws ExceptionDBGit {
        super(sqlObject);
    }

    /**
     * @return Type meta object
     */
    @Override
    public final IDBGitMetaType getType() {
        return DBGitMetaType.DBGitDomain;
    }

    /**
     * load current object from DB
     */
    @Override
    public final boolean loadFromDB() throws ExceptionDBGit {
        final IDBAdapter adapter = AdapterFactory.createAdapter();
        final NameMeta nm = MetaObjectFactory.parseMetaName(name);
        final DBDomain dbObject = adapter.getDomain(nm.getSchema(), nm.getName());

        if (dbObject != null) {
            setSqlObject(dbObject);
            return true;
        } else
            return false;
    }
}
