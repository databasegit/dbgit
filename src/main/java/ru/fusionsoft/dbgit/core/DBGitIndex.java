package ru.fusionsoft.dbgit.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBGitIndex {
	public static final String VERSION = "0.3.1";
	
	private static DBGitIndex gitIndex = null;
	private TreeMapItemIndex treeItems;
	private boolean hasConflicts = false;
	private String version = "";
	
	private DBGitIndex() throws ExceptionDBGit {
		treeItems = new TreeMapItemIndex();
		loadDBIndex();
	}
	
	public static DBGitIndex getInctance() throws ExceptionDBGit {
		if (gitIndex == null) {
			gitIndex = new DBGitIndex();
		}
		return gitIndex;
	}
	
	protected ItemIndex getItemIndexExistsOrNew(String name) {
		if (treeItems.containsKey(name)) {
			return treeItems.get(name);
		}
		
		ItemIndex item = new ItemIndex();
		item.setName(name);
		return item;
	}
	
	public ItemIndex getItemIndex(String name) {
		return treeItems.get(name);
	}

	public TreeMapItemIndex getTreeItems() {
		return treeItems;
	}
	
	public String getRepoVersion() {
		return version;
	}
	
	protected ItemIndex editItem(IMetaObject obj, Boolean isDelete) {
		ItemIndex item = getItemIndexExistsOrNew(obj.getName());
		item.setIsDelete(isDelete);
		item.setHash(obj.getHash());
		treeItems.put(obj.getName(), item);
		return item;
	}
	
	public ItemIndex addItem(IMetaObject obj) {
		return editItem(obj, false);
	}
	
	public ItemIndex deleteItem(IMetaObject obj) {
		return editItem(obj, true);
	}

	public boolean removeItemFromIndex(IMetaObject obj) {
		String name = obj.getName();
		return removeItemFromIndex(name);
	}

	public boolean removeItemFromIndex(String name) {
		try {
			treeItems.remove(name);
			addToGit();
			saveDBIndex();
			return true;
		} catch (Exception ex){
			ex.printStackTrace();
			return false;
		}
	}

	public void saveDBIndex() throws ExceptionDBGit {
		try{				
			File file = new File(DBGitPath.getFullPath(DBGitPath.INDEX_FILE));				
			DBGitPath.createDir(file.getParent());
			FileWriter writer = new FileWriter(file.getAbsolutePath());		
			writer.write("version=" + VERSION + "\n");
		    for (ItemIndex item : treeItems.values()) {
		    	writer.write(item.toString()+"\n");
		    }			
			writer.close();		    
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public void loadDBIndex() throws ExceptionDBGit {
		try{				
			File file = new File(DBGitPath.getFullPath(DBGitPath.INDEX_FILE));
			
			if (!file.exists()) {
				version = "new";
				return ;
			}
			BufferedReader br = new BufferedReader(new FileReader(file));			
			for(String line; (line = br.readLine()) != null; ) {
				
				if (line.startsWith("<<<<<<<") || line.startsWith("=======") || line.startsWith(">>>>>>>")) {
					hasConflicts = true;
					continue;
				}
				
				if (line.startsWith("version=")) {
					version = line.substring(8);
				} else {				
				    ItemIndex item = ItemIndex.parseString(line);
				    treeItems.put(item.getName(), item);
				}
			}
			    
			br.close();		    
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public void addToGit() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.INDEX_FILE);
		dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.DB_LINK_FILE);
	}
	
	public void addLinkToGit() throws ExceptionDBGit {
		File file = new File(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.DB_LINK_DEF_FILE);
		if (file.exists())
			DBGit.getInstance().addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.DB_LINK_DEF_FILE);
	}
	
	public void addIgnoreToGit() throws ExceptionDBGit {
		DBGit.getInstance().addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.DB_IGNORE_FILE);
	}
	
	public boolean hasConflicts() {
		return hasConflicts;
	}
	
	public boolean isCorrectVersion() {
		if (version.equals("new"))
			return true;
		if (version == null || version.equals(""))
			return false;
		else
			return version.equals(VERSION);
	}
}

