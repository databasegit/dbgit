package ru.fusionsoft.dbgit.yaml;

import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

import ru.fusionsoft.dbgit.utils.StringProperties;

/**
 * <div class="en">A class that allows you to use your own constructor for an object from yaml</div>
 * <div class="ru">Класс, позволяющий использовать собственный конструктор для объекта из yaml</div>
 * @author mikle
 *
 */
public class DBGitYamlConstructor extends Constructor {
	protected final Map<Class<?>, Construct> constructorsByClass = new HashMap<Class<?>, Construct>();
	
	public DBGitYamlConstructor() {
		constructorsByClass.put(StringProperties.class, new ConstructYamlStringProperties());
	}
	
	@Override
    protected Construct getConstructor(Node node) {
    	if (constructorsByClass.containsKey(node.getType())) {
    		return constructorsByClass.get(node.getType());
    	}
    	
    	return super.getConstructor(node);
    }
    
    /*
    @Override
    protected Object constructObject(Node node)
    */
	
	
    private class ConstructYamlStringProperties extends AbstractConstruct { 
        @Override
        public Object construct(Node node) {
        	if (node instanceof MappingNode) {
	        	Object obj = constructMapping((MappingNode)node);
	    		
	    		StringProperties properties = new StringProperties();
	    		parseMap(properties, obj);
	        	
	        	return properties;
        	}
        	if (node instanceof ScalarNode) {
	        	
	    		//TODO
	    		StringProperties properties = new StringProperties();
	        	
	        	return properties;
        	}
        	return null;
        }
        
        @SuppressWarnings("unchecked")
        public void parseMap(StringProperties pr, Object obj) {
        	if (obj instanceof Map) {
        		Map<String, String> map =  (Map<String, String>)obj;
        		for( String key : map.keySet()) {        			
        			StringProperties newPr = (StringProperties)pr.addChild(key);
        			parseMap(newPr, map.get(key));
        		}
        	} else {        		
        		pr.setData((String)obj);
        	}
        }
    }
}