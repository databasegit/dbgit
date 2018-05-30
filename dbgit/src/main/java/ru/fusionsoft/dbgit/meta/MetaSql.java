package ru.fusionsoft.dbgit.meta;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

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
	public boolean serialize(OutputStream stream) throws Exception {
		stream.write(getSqlObject().getSql().getBytes(Charset.forName("UTF-8")));
		return true;
	}

	@Override
	public IMetaObject deSerialize(InputStream stream) throws Exception {
		NameMeta nm = MetaObjectFactory.parseMetaName(getName());
		sqlObject = new DBSQLObject();		
		sqlObject.setName(nm.getName());
		sqlObject.setSchema(nm.getSchema());
		sqlObject.setSql(IOUtils.toString(stream, StandardCharsets.UTF_8.name()));
		
		return this;
	}

	@Override
	public String getHash() {
		return sqlObject.getHash();
	}

}
