package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.dbobjects.DBOptionsObject;
import ru.fusionsoft.dbgit.utils.CalcHash;

/**
 * Base Meta class for data use DBOptionsObject information. This data is tree string properties.
 * @author mikle
 *
 */
public class MetaObjOptions extends MetaBase {
	private DBGitMetaType type;
	private DBOptionsObject objectOption = null;
	
	public DBOptionsObject getObjectOption() {
		return objectOption;
	}

	public void setObjectOption(DBOptionsObject objectOption) {
		this.objectOption = objectOption;
		setName(objectOption.getName()+"."+getType().getValue());
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
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		ch.addData(this.getType().getValue());
		ch.addData(objectOption.getHash());

		return ch.calcHashStr();
	}

}
