package ru.fusionsoft.dbgit.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

/**
 * <div class="en">
 * A class that implements the meta description of an object. 
 * These objects store information about the objects of the database. 
 * Saved on the disk under the control of git.
 * Used by adapters to restore the state of the database.
 * </div>
 * 
 * <div class="ru">
 * Класс реализующий мета описание объекта. 
 * Данные объекты хранят в себе информацию об объектах БД. 
 * Сохраняются на диске под управлением гит.
 * Используются адаптерами для восстановления состояния БД.
 * </div>
 * 
 * @author mikle
 *
 */
public interface IMetaObject {
	/**
	 * 
	 * @return Type meta object
	 */
	public DBGitMetaType getType();
	
	/**
	 * Full name meta object with path and extension type  
	 * Example myschema/mytable.tbl 
	 * @return
	 */
	public String getName();
	
	public void setName(String name);
	
	public String getFileName();
		
	/**
	 * Save data to stream
	 * @param stream
	 * @throws IOException
	 */
	public void serialize(OutputStream stream) throws IOException;
	
	/**
	 * Load object from stream
	 * @param stream
	 * @return New object current class
	 * @throws IOException
	 */
	public IMetaObject deSerialize(InputStream stream) throws IOException;
	
	/**
	 * load current object from DB
	 */
	public void loadFromDB() throws ExceptionDBGit;
		
	public String getHash();
	
	/**
	 * Save meta file to base path
	 * Example - .dbgit/basePath/path_and_filename_meta_object
	 * 
	 * @param basePath
	 * @throws IOException
	 */
	default void saveToFile(String basePath) throws IOException, ExceptionDBGit {
		File file = new File(DBGitPath.getFullPath(basePath)+"/"+getFileName());
		DBGitPath.createDir(file.getParent());
				
		FileOutputStream out = new FileOutputStream(file.getAbsolutePath());
		this.serialize(out);
		out.close();
		
		ConsoleWriter.println("Write file object: "+getName());
	}
	
	default void saveToFile() throws IOException, ExceptionDBGit {
		saveToFile(null);
	}
	
	/**
	 * Load meta file to base path
	 * Example - .dbgit/basePath/path_and_filename_meta_object
	 * 
	 * @param basePath
	 * @throws IOException
	 */
	default IMetaObject loadFromFile(String basePath) throws IOException, ExceptionDBGit {
		String filename = DBGitPath.getFullPath(basePath);
		File file = new File(filename+"/"+getFileName());
		FileInputStream fis = new FileInputStream(file);
		IMetaObject meta = this.deSerialize(fis);
		fis.close();
		if (meta != null) {
			meta.setName(this.getName());
		}
		
		return meta;
	}
	
	default IMetaObject loadFromFile() throws IOException, ExceptionDBGit {
		return loadFromFile(null);
	}
}
