package ru.fusionsoft.dbgit.meta;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Meta class for sql data
 * @author mikle
 *
 */
public class MetaSql extends MetaBase {

	public MetaSql(String name) {
		
	}
	
	@Override
	public DBGitMetaType getType() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void serialize(OutputStream stream) {
		// TODO Auto-generated method stub

	}

	@Override
	public IMetaObject deSerialize(InputStream stream) {
		// TODO Auto-generated method stub
		return null;
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
