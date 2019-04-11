package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBGit {
	private static DBGit dbGit = null;
	private Repository repository;
	private Git git;

	private DBGit() throws ExceptionDBGit {
		try {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			repository = builder
			  .readEnvironment() // scan environment GIT_* variables
			  .findGitDir() // scan up the file system tree
			  .build();	
			
			git = new Git(repository);
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
	public static DBGit getInstance() throws ExceptionDBGit {
		if (dbGit == null) {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			
			if (builder.readEnvironment().findGitDir().getGitDir() == null) {
				ConsoleWriter.printlnRed( "Git repository not found");				
				System.exit(0);
			}
			
			dbGit = new DBGit();
		}
		return dbGit;
	}

	public Repository getRepository() {
		return repository;
	}

	public void setRepository(Repository repository) {
		this.repository = repository;
	}
	
	public String getRootDirectory() {
		return repository.getDirectory().getParent();
	}
	
	/**
	 * Get list git index files by path. 
	 * 
	 * @param path
	 * @return
	 */
	public List<String> getGitIndexFiles(String path) throws ExceptionDBGit {
		try {
			DirCache cache = repository.readDirCache();
			List<String> files = new ArrayList<String>();
			Integer pathLen = path.length();
			if (!(path.endsWith("/") || path.endsWith("\\") || path.equals(""))) {
				pathLen++;
			}
	    	    	
	    	for (int i = 0; i < cache.getEntryCount(); i++) {
	    		String file = cache.getEntry(i).getPathString();
	    		
	    		//System.out.rintln(cache.getEntry(i).getPathString() +"   "+cache.getEntry(i).getObjectId().getName());
	    		
	    		
	    		if (file.startsWith(path)) {
	    			files.add(file.substring(pathLen));
	    		}	    		
	    	}
	    	
	    	return files;
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
	public List<String> getAddedObjects(String path) throws ExceptionDBGit {
		try {
			List<String> files = new ArrayList<String>();
			Integer pathLen = path.length();
			if (!(path.endsWith("/") || path.endsWith("\\") || path.equals(""))) {
				pathLen++;
			}
			
			Status st = git.status().call();
	    	for (String file : st.getAdded()) {
	    		if (file.startsWith(path)) {
	    			files.add(file.substring(pathLen));
	    		}	
	    	}
	    	
	    	return files;
		} catch (Exception e) {
	    	throw new ExceptionDBGit(e);
	    } 
	}
	
	public void addFileToIndexGit(String filename) throws ExceptionDBGit {
		//https://github.com/centic9/jgit-cookbook/blob/master/src/main/java/org/dstadler/jgit/porcelain/AddFile.java
        try {
        	/*
        	System.out.rintln(repository.getBranch());        	
        	System.out.rintln(filename);
        	 */
            git.add().addFilepattern(filename).call();
        } catch (Exception e) {
        	throw new ExceptionDBGit(e);
        }         
	}
	
	public void removeFileFromIndexGit(String filename) throws ExceptionDBGit {
		try {        	      
            git.rm().addFilepattern(filename).call();           
        } catch (Exception e) {
        	throw new ExceptionDBGit(e);
        } 
	}
	
	public void gitCommit(boolean existsSwitchA, String msg, String path) throws ExceptionDBGit {
		try {
			if (existsSwitchA) {
				GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
				IMapMetaObject fileObjs = gmdm.loadFileMetaData();
				DBGitIndex index = DBGitIndex.getInctance();

				for (IMetaObject obj : fileObjs.values()) {
					String hash = obj.getHash();
					gmdm.loadFromDB(obj);
					
					if (!obj.getHash().equals(hash)) {
						obj.saveToFile();
						index.addItem(obj);
						obj.addToGit();
					}			
				}
				
				index.saveDBIndex();
				index.addToGit();
			}
			
			if (path == null || path.length() == 0) {
				if (msg.length() > 0 ) {
					git.commit().setAll(existsSwitchA).setMessage(msg).call();
				} else {
					git.commit().setAll(existsSwitchA).call();
				}				
			} else {
				if (msg.length() > 0 ) {
					git.commit().setAll(existsSwitchA).setOnly(DBGitPath.DB_GIT_PATH + "/" + path).setMessage(msg).call();
				} else {
					git.commit().setAll(existsSwitchA).setOnly(DBGitPath.DB_GIT_PATH + "/" + path).call();
				}								
			}
			
        } catch (Exception e) {
        	throw new ExceptionDBGit(e);
        } 
	}

}
