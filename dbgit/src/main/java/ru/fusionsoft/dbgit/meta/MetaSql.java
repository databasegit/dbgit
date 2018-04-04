package ru.fusionsoft.dbgit.meta;

import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;

/**
 * Meta class for sql data
 * @author mikle
 *
 */
public abstract class MetaSql extends MetaBase {
	protected DBSQLObject sqlObject;
	
	public MetaSql() {}
	
	public MetaSql(DBSQLObject sqlObject) {
		this();
		setSqlObject(sqlObject);
	}	
	
	public DBSQLObject getSqlObject() {
		return sqlObject;
	}


	public void setSqlObject(DBSQLObject sqlObject) {
		this.sqlObject = sqlObject;
		setName(sqlObject.getSchema()+"/"+sqlObject.getName()+"."+getType().getValue());
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
	public String getHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
