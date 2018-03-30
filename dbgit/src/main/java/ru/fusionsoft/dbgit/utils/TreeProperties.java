package ru.fusionsoft.dbgit.utils;

import java.util.HashMap;
import java.util.Map;


public class TreeProperties<T> {
	private T data = null;
	private TreeProperties<T> parent = null;
    private Map<String, TreeProperties<T>> children = null;

    public TreeProperties() {        
        this.children = new HashMap<String, TreeProperties<T>>();
    }
    
    public TreeProperties(T data) {        
        this();
        this.data = data;
    }
    
    public TreeProperties<T> newInstance() {
    	try {
    		return this.getClass().newInstance();
    	} catch (Exception e) {
    		throw new RuntimeException(e);
    	}
    }
    
    public TreeProperties<T> addChild(String name) {
    	TreeProperties<T> childNode = newInstance();// new TreeProperties<T>();
        childNode.parent = this;
        this.children.put(name, childNode);
        return childNode;
    }
    
    public TreeProperties<T> addChild(String name, TreeProperties<T> properties) {
    	this.children.put(name, properties);
    	properties.parent = this;
        return properties;
    }

    public TreeProperties<T> addChild(String name, T val) {
    	TreeProperties<T> childNode = addChild(name);
    	childNode.setData(val);
        return childNode;
    }
    
    public TreeProperties<T> deleteChild(String name) {
    	if (children.containsKey(name)) {
    		children.remove(name);
    	}
    	return this;
    }
    
    public TreeProperties<T> xPath(String path) {
    	Integer pos = path.indexOf("/");
    	if (pos >= 0) {
    		String nm = path.substring(0, pos);
    		TreeProperties<T> ch = get(nm);
    		if (ch != null) {
    			return ch.xPath(path.substring(pos+1));
    		}
    	} else {
    		return get(path);
    	}
    	return null;
    } 

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public TreeProperties<T> getParent() {
		return parent;
	}

	public Map<String, TreeProperties<T>> getChildren() {
		return children;
	}
	
	public TreeProperties<T> get(String name) {
		if (children.containsKey(name)) {
			return children.get(name);
    	}
    	return null;
	}
	
	public void setChildren(Map<String, TreeProperties<T>> children) {
		if (children != null) {
			this.children = children;
		}
	}

}


