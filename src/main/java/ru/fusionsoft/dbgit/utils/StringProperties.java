package ru.fusionsoft.dbgit.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

/**
 * Tree string properties 
 * 
 * @author mikle
 *
 */
public class StringProperties {
	private String data = null;
	private StringProperties parent = null;
    private Map<String, StringProperties> children = null;

    public StringProperties() {        
        this.children = new TreeMap<String, StringProperties>();
    }
    
    public StringProperties(String data) {        
        this();
        this.data = data;
    }
    
    
    public StringProperties addChild(String name) {
    	StringProperties childNode = new StringProperties();
        childNode.parent = this;
        this.children.put(name, childNode);
        return childNode;
    }
    
    public StringProperties addChild(String name, StringProperties properties) {
    	this.children.put(name, properties);
    	properties.parent = this;
        return properties;
    }

    public StringProperties addChild(String name, String val) {
    	StringProperties childNode = addChild(name);
    	childNode.setData(val);
        return childNode;
    }
    
    public StringProperties deleteChild(String name) {
    	if (children.containsKey(name)) {
    		children.remove(name);
    	}
    	return this;
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

	public StringProperties getParent() {
		return parent;
	}

	public Map<String, StringProperties> getChildren() {
		return children;
	}
	
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

}



