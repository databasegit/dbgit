package ru.fusionsoft.dbgit.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.core.DBGitPath;

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
	public void loadFromDB();
		
	public String getHash();
	
	/**
	 * Save meta file to base path
	 * Example - .dbgit/basePath/path_and_filename_meta_object
	 * 
	 * @param basePath
	 * @throws IOException
	 */
	default void saveToFile(String basePath) throws IOException {
		String filename = DBGitPath.getFullPath(basePath);
		FileOutputStream out = new FileOutputStream(filename);
		this.serialize(out);
		out.close();
		
		System.out.println("write file "+filename);
	}
	
	/**
	 * Load meta file to base path
	 * Example - .dbgit/basePath/path_and_filename_meta_object
	 * 
	 * @param basePath
	 * @throws IOException
	 */
	default IMetaObject loadFromFile(String basePath) throws IOException {
		String filename = DBGitPath.getFullPath(basePath);
		File file = new File(filename);
		FileInputStream fis = new FileInputStream(file);
		IMetaObject meta = this.deSerialize(fis);
		fis.close();
		
		System.out.println("read file " + filename);
		return meta;
	}
}
