package ru.fusionsoft.dbgit.meta;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Class for meta data of blob field value
 * @author mikle
 *
 */
public class MetaBlobData extends MetaBase {

	public MetaBlobData(String name) {
		
	}
	
	@Override
	public DBGitMetaType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean serialize(OutputStream stream) {
		// TODO Auto-generated method stub
		return false;

	}

	@Override
	public IMetaObject deSerialize(InputStream stream) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean loadFromDB() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
