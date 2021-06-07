package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBEnum;

public class MetaEnum extends MetaSql {
    /**
     * @return Type meta object
     */
    @Override
    public IDBGitMetaType getType() {
        return DBGitMetaType.DBGitEnum;
    }

    /**
     * load current object from DB
     */
    @Override
    public boolean loadFromDB() throws ExceptionDBGit {
        final IDBAdapter adapter = AdapterFactory.createAdapter();
        final NameMeta nm = MetaObjectFactory.parseMetaName(name);
        final DBEnum dbObject = adapter.getEnum(nm.getSchema(), nm.getName());

        if (dbObject != null) {
            setSqlObject(dbObject);
            return true;
        } else
            return false;
    }
}
