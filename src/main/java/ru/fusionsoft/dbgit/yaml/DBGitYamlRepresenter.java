package ru.fusionsoft.dbgit.yaml;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.DumperOptions.ScalarStyle;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.meta.MetaSql;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;


/**
 * <div class="en">A class that allows you to use your own view for an object in yaml</div>
 * <div class="ru">Класс, позволяющий использовать собственное представление для объекта в yaml</div>
 * @author mikle
 *
 */
public class DBGitYamlRepresenter extends Representer {
	
	static class ComparatorProperty implements Comparator<Property> {
		public static final int MAX_ORDER = 1000;
		
		private Map<String, Integer> propertyOrders = new HashMap<>();
		
		public ComparatorProperty(Class<?> cl) {				
			List<Field> fields = new ArrayList<Field>();
			for(Field field : getAllFields(fields, cl)){
			  String name = field.getName();
			  Annotation[] annotations = field.getDeclaredAnnotations();
			  
			  for (int i = 0; i < annotations.length; i++) {
				  if (annotations[i] instanceof YamlOrder) {	  
					  YamlOrder o = (YamlOrder) annotations[i];
					  propertyOrders.put(name, o.value());
				  }
			  }
			}
		}
		
		public List<Field> getAllFields(List<Field> fields, Class<?> type) {
		    fields.addAll(Arrays.asList(type.getDeclaredFields()));

		    if (type.getSuperclass() != null) {
		        getAllFields(fields, type.getSuperclass());
		    }

		    return fields;
		}
		
		@Override
		public int compare(Property p1, Property p2) {
			int order1 = propertyOrders.containsKey(p1.getName()) ? propertyOrders.get(p1.getName()) : MAX_ORDER;
			int order2 = propertyOrders.containsKey(p2.getName()) ? propertyOrders.get(p2.getName()) : MAX_ORDER;
			
			int diff = order1 - order2;
			if (diff != 0) return diff;
			
			return p1.compareTo(p2);
		}
	}
	
	@Override
	protected MappingNode representJavaBean(Set<Property> properties, Object javaBean) {
        List<NodeTuple> value = new ArrayList<NodeTuple>(properties.size());
        Tag tag;
        tag = Tag.MAP;
        // flow style will be chosen by BaseRepresenter
        MappingNode node = new MappingNode(tag, value, FlowStyle.AUTO);
        representedObjects.put(javaBean, node);
        DumperOptions.FlowStyle bestStyle = FlowStyle.FLOW;
        for (Property property : properties) {
            Object memberValue = property.get(javaBean);
            Tag customPropertyTag = memberValue == null ? null
                    : classTags.get(memberValue.getClass());
            NodeTuple tuple = representJavaBeanProperty(javaBean, property, memberValue,
                    customPropertyTag);
            if (tuple == null) {
                continue;
            }
            if (!((ScalarNode) tuple.getKeyNode()).isPlain()) {
                bestStyle = FlowStyle.BLOCK;
            }
            Node nodeValue = tuple.getValueNode();
            if (!(nodeValue instanceof ScalarNode && ((ScalarNode) nodeValue).isPlain())) {
                bestStyle = FlowStyle.BLOCK;
            }
            value.add(tuple);
        }
        if (defaultFlowStyle != FlowStyle.AUTO) {
            node.setFlowStyle(defaultFlowStyle);
        } else {
            node.setFlowStyle(bestStyle);
        }
        return node;
    }
	
	public DBGitYamlRepresenter() {
        this.representers.put(StringProperties.class, new RepresentYamlStringProperties());
        this.representers.put(String.class, new RepresentYamlString());
    }
	
	@Override
	protected Set<Property> getProperties(Class<? extends Object> type) {
		Set<Property> result = new TreeSet<Property>(/*Collections.reverseOrder()*/ new ComparatorProperty(type));
        result.addAll(super.getProperties(type));
        return result;
	}

	private class RepresentYamlString implements Represent {

		@Override
		public Node representData(Object data) {
			String dt = (String) data;
			
        	if (dt.contains("\n") || dt.contains("\r")) {
        		return representScalar(Tag.STR, dt, ScalarStyle.LITERAL);
        	}
        		
            return representScalar(Tag.STR, dt);		
		}
		
	}
	
	private class RepresentYamlStringProperties implements Represent {
		public Node representData(Object data) {        
        	StringProperties tree = (StringProperties) data;        
        	
        	if (tree.getChildren().size() > 0) {        		
        		return representMapping(Tag.MAP, tree.getChildren(), DumperOptions.FlowStyle.BLOCK);        		
        	}
        	
        	if (tree.getData() == null) {
        		return representScalar(Tag.STR, "");
        	}       	
        	
        	if (tree.getData().contains("\n") || tree.getData().contains("\r")) {
        		return representScalar(Tag.STR, tree.getData(), ScalarStyle.LITERAL);
        	}
        		
            return representScalar(Tag.STR, tree.getData());
        }
    }
}
