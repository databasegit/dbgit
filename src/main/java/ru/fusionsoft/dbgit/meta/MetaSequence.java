package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSchemaObject;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.utils.CalcHash;

public class MetaSequence extends MetaBase {
	private DBSequence sequence;
		
	public MetaSequence() {
		super();
	}
	
	public MetaSequence(DBSequence seq) {
		this.sequence = seq;
	}
	
	@Override
	public boolean serialize(OutputStream stream) throws IOException {
		return yamlSerialize(stream);
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws IOException{
		return yamlDeSerialize(stream);
	}
	
	@Override
	public void setName(String name) {
		this.name = name; 
		
	}
	
	@Override
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		ch.addData(this.getSequence().getHash());
		return ch.calcHashStr();
	}
	
	public DBSequence getSequence() {
		return sequence;
	}

	public void setSequence(DBSequence sequence) {
		this.sequence = sequence;
		setName(sequence.getSchema()+"/"+sequence.getName()+"."+getType().getValue());
	}

	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitSequence;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());

		DBSequence seq = adapter.getSequence(nm.getSchema(), nm.getName());
		setSequence(seq);
		
		return true;
	}


}
