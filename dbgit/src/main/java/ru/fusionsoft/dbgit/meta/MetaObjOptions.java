package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.dbobjects.DBOptionsObject;

public class MetaObjOptions extends MetaBase {
	private DBGitMetaType type;
	private DBOptionsObject objectOption = null;
	
	public DBOptionsObject getObjectOption() {
		return objectOption;
	}

	public void setObjectOption(DBOptionsObject objectOption) {
		this.objectOption = objectOption;
	}

	@Override
	public DBGitMetaType getType() {
		return type;
	}
	
	public void setType(DBGitMetaType type) {
		this.type = type;
	}
	
	@Override
	public void serialize(OutputStream stream) throws IOException {
		yamlSerialize(stream);
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws IOException{
		return yamlDeSerialize(stream);
	}

	@Override
	public void loadFromDB() {
		// TODO Auto-generated method stub

	}

	@Override
	public String getHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
