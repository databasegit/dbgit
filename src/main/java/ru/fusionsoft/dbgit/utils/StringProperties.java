package ru.fusionsoft.dbgit.utils;

import java.sql.ResultSet;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;

/**
 * Tree string properties 
 * 
 * @author mikle
 *
 */
public class StringProperties {
	private String data;
    private Map<String, StringProperties> children;

    public StringProperties() {        
        this.children = new TreeMap<>();
        this.data = "";
    }
    
    public StringProperties(String data) {
		this.children = new TreeMap<>();
		this.data = data;
    }
    public StringProperties(Map<String, StringProperties> children) {
		this.children = children == null ? new TreeMap<>() : children;
		this.data = "";
    }

	public StringProperties(ResultSet rs) {
		this.children = new TreeMap<>();
		this.data = "";
		try {
			final int columnCount = rs.getMetaData().getColumnCount();
			for (int i = 1; i <= columnCount; i++) {

				final String columnName = rs.getMetaData().getColumnName(i);
				final String columnValue = rs.getString(i);

				if (columnName.equalsIgnoreCase("dependencies")) continue ;
				if (columnValue == null) continue ;

				final String cleanValue = cleanString(columnValue);
				final String cleanName = columnName.toLowerCase();

				addChild(cleanName, cleanValue);
			}
		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}

    
    public void addChild(String name, String stringValue) {
    	StringProperties valueOnlyNode = new StringProperties(stringValue);
		this.children.put(name, valueOnlyNode);
    }

    public void addChild(String name, StringProperties value) {
		this.children.put(name, value);
    }
    
    public void deleteChild(String name) {
    	if (children.containsKey(name)) {
    		children.remove(name);
    	}
    }
    
    public StringProperties xPath(String path) {
    	Integer pos = path.indexOf("/");
    	if (pos >= 0) {
    		String nm = path.substring(0, pos);
    		StringProperties ch = get(nm);
    		if (ch != null) {
    			return ch.xPath(path.substring(pos+1));
    		}
    	} else {
    		return get(path);
    	}
    	return null;
    } 

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Map<String, StringProperties> getChildren() {
		return children;
	}

	//TODO @Nullable
	public StringProperties get(String name) {
		if (children.containsKey(name)) {
			return children.get(name);
    	}
    	return null;
	}
	
	public void setChildren(Map<String, StringProperties> lst) {
		if (lst != null) {
			children.clear();
			children.putAll(lst);			
		}
	}
	
	public StringBuilder toString(Integer level) {
		StringBuilder sb = new StringBuilder("");
		String prefix = StringUtils.leftPad("", 4*level, " ");
		if (children.size() > 0) {
			for (String item : children.keySet()) {
				sb.append("\n"+prefix+item+":"+" "+children.get(item).toString(level+1));
			}
		} else {
			sb.append(getData());
		}
		
		return sb;
	}
	
	public String toString() {
		return toString(0).toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		StringProperties that = (StringProperties) o;
		if(getData() != null){
			return getData().equals(that.getData())
			&& Objects.equals(getChildren(), that.getChildren());
		} else {
			return that.getData() == null
			&& Objects.equals(getChildren(), that.getChildren());
		}


	}

	@Override
	public int hashCode() {
		return Objects.hash(getData(), getChildren());
	}

	public String cleanString(String str) {
		String dt = str.replace("\r\n", "\n");
		while (dt.contains(" \n")) dt = dt.replace(" \n", "\n");
		dt = dt.replace("\t", "   ").trim();

		return dt;
	}
}



