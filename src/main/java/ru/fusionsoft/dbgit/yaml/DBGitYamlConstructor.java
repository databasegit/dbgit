package ru.fusionsoft.dbgit.yaml;

import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
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

				final Map<Object, Object> map = constructMapping((MappingNode)node);
	        	return fromTreeNode(map);
        	}
        	if (node instanceof ScalarNode) {
	        	
				final String stringValue = constructScalar((ScalarNode)node).toString();
				return new StringProperties(stringValue);
        	}

        	throw new ExceptionDBGitRunTime("return null in construct! Gotcha!");
//        	return null;
        }

		public StringProperties fromTreeNode(Object treeNode) {
			try {
				final Map<String, Object> map = (Map<String, Object>) treeNode;
				final StringProperties constructedNode = new StringProperties();

				map.forEach((key, value) -> constructedNode.addChild(key, fromTreeNode(value)));
				return constructedNode;

			} catch (ClassCastException e) {
				return new StringProperties(treeNode.toString());
			}
		}
    }
}