package ru.fusionsoft.dbgit.yaml;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import ru.fusionsoft.dbgit.utils.StringProperties;

/**
 * <div class="en">A class that allows you to use your own view for an object in yaml</div>
 * <div class="ru">Класс, позволяющий использовать собственное представление для объекта в yaml</div>
 * @author mikle
 *
 */
public class DBGitYamlRepresenter extends Representer {
	public DBGitYamlRepresenter() {        
        this.representers.put(StringProperties.class, new RepresentYamlStringProperties());
    }
	
	@Override
	protected Set<Property> getProperties(Class<? extends Object> type) {
		
		
		if (type.equals(StringProperties.class)) {
			//order for table 
			Set<Property> result = new TreeSet<Property>(Collections.reverseOrder());
	        result.addAll(super.getProperties(type));
	        return result;
		}

		return super.getProperties(type);
	}
	
	private class RepresentYamlStringProperties implements Represent {
		public Node representData(Object data) {        
        	StringProperties tree = (StringProperties) data;        
        	
        	if (tree.getChildren().size() > 0) {
        		return representMapping(Tag.MAP, tree.getChildren(), DumperOptions.FlowStyle.AUTO);
        	}
        	if (tree.getData() == null) {
        		return representScalar(Tag.STR, "");
        	}
        	
            return representScalar(Tag.STR, tree.getData());
        }
    }
}
