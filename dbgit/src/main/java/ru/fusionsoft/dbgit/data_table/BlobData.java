package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import java.sql.Blob;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;


public class BlobData implements ICellData {
	private Blob value;
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {		
		value = rs.getBlob(fieldname);
		if (rs.wasNull()) {
			value = null;
	    }
		return true;
	}
	
	@Override
	public String serialize(DBTable tbl) throws Exception {
		return convertToString();
	}
	
	@Override
	public void deserialize(String data) throws Exception {
		byte[] st = (byte[]) data.getBytes();
		this.value = null;
		this.value.setBytes(1, st);
		
	    /*ByteArrayInputStream baip = new ByteArrayInputStream(st);
		ObjectInputStream ois = new ObjectInputStream(baip);
        try {
            this.value = (Blob) ois.readObject();
        } finally {
            ois.close();
        }*/
	}
	
	@Override
	public String convertToString() {		 
		return value != null ?  value.toString() : null;
	}
	
	public Blob getValue() {
		return value;
	}

	public void setValue(Blob value) {
		this.value = value;
	}

	public Object getWriterForRapair() {
		return null;
	}
	
	public int addToGit() {
		return 0;
	}
	
	public int removeFromGit() {
		return 0;
	}

	@Override
	public String getSQLData() {
		String data = "\'"+value.toString()+"\'";
		return data;
	}
	
}
