package ru.fusionsoft.dbgit.data_table;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.ResultSet;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.Convertor;

public class MapFileData implements ICellData {
	private String srcFile = null;
	private File tmpFile = null;
	private String hash = null;
		
	public InputStream getBlobData(ResultSet rs, String fieldname) throws Exception {
		return rs.getBinaryStream(fieldname);
	}
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {
		InputStream stream = getBlobData(rs, fieldname);		
		
		if (stream != null) {
			tmpFile = new File(DBGitPath.getTempDirectory()+"/"+Convertor.getGUID());
			Files.copy(stream, tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);	    
		}
		return true;
	}
	
	@Override
	public String serialize(DBTable tbl) throws Exception {
		if (tmpFile != null) {
			srcFile = tbl.getSchema()+"/"+tbl.getName()+"/"+getHash()+".data";
			
			File wrtFile = new File(DBGitPath.getFullPath()+"/"+srcFile);
			DBGitPath.createDir(wrtFile.getAbsolutePath());
			
			Files.move(tmpFile.toPath(), wrtFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "dataTable", "writeData").withParams(srcFile));
			tmpFile = null;
		}
		return srcFile;
	}
	
	@Override
	public void deserialize(String data) throws Exception {
		File fp = new File(DBGitPath.getFullPath()+"/"+data);
		if (fp.exists()) {
			this.srcFile = data;
		}
	}
	
	@Override
	public String convertToString() throws Exception {
		return getHash();
	}
	
	public Object getWriterForRapair() {
		return null;
	}
	
	public String getHash() throws Exception {
		if (hash == null) {
			String path = null;
			if (tmpFile != null) path = tmpFile.getAbsolutePath();
			if (srcFile != null) path = DBGitPath.getFullPath()+"/"+srcFile;
			
			if (path == null) return "";
			
			CalcHash ch = new CalcHash();
			ch.addDataFile(path);
			hash = ch.calcHashStr();
		}
		
		return hash;
	}
	
	public int addToGit() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		if (srcFile != null) {
			dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+srcFile);
			return 1;
		}
		return 0;
	}
	
	public int removeFromGit() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		if (srcFile != null) {
			dbGit.removeFileFromIndexGit(DBGitPath.DB_GIT_PATH+"/"+srcFile);
			return 1;
		}
		return 0;
	}

	@Override
	public String getSQLData() {
		// TODO Auto-generated method stub
		return srcFile;
	}
	
	public File getFile() throws ExceptionDBGit {
		return new File(DBGitPath.getFullPath() + srcFile);
	}
		
}

