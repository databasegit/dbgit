package ru.fusionsoft.dbgit.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import ru.fusionsoft.dbgit.meta.IMetaObject;

public class DBGitIndex {
	private static DBGitIndex gitIndex = null;
	private TreeMapItemIndex treeItems;
	
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
	
	public void saveDBIndex() throws ExceptionDBGit {
		try{				
			File file = new File(DBGitPath.getFullPath(DBGitPath.INDEX_FILE));				
			DBGitPath.createDir(file.getParent());
			FileWriter writer = new FileWriter(file.getAbsolutePath());		
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
			
			if (!file.exists()) return ;
			
			BufferedReader br = new BufferedReader(new FileReader(file));			
			for(String line; (line = br.readLine()) != null; ) {
			    ItemIndex item = ItemIndex.parseString(line);
			    treeItems.put(item.getName(), item);
			}
			    
			br.close();		    
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public void addToGit() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		dbGit.addFileToIndexGit(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.INDEX_FILE);
	}
}

