package ru.fusionsoft.dbgit.core;

public class ItemIndex {
	private String name;
	private String hash;
	private Boolean isDelete;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHash() {
		return hash;
	}
	public void setHash(String hash) {
		this.hash = hash;
	}
	public Boolean getIsDelete() {
		return isDelete;
	}
	public void setIsDelete(Boolean isDelete) {
		this.isDelete = isDelete;
	}
	
	public String toString() {
		return name+";"+hash+";"+isDelete.toString();
	}
	
	public static ItemIndex parseString(String line) throws ExceptionDBGit  {
		try {
			String tmp[] = line.split(";");
			ItemIndex item = new ItemIndex();
			item.setName(tmp[0]);
			item.setHash(tmp[1]);
			item.setIsDelete(Boolean.valueOf(tmp[2]));
			return item;
		} catch(Exception e) {
			throw new ExceptionDBGit("Error parse ItemIndex!", e);
		}  
	}
}
