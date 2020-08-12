package ru.fusionsoft.dbgit.meta;

import com.diogonunes.jcdp.color.api.Ansi;
import com.google.common.collect.Sets;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Sorted List Meta Object (for sorting and stuff)
 *
 * @author rtm786
 *
 */
public class SortedListMetaObject {
    private List<IMetaObject> listFromDependant;
    private List<IMetaObject> listFromFree;
    private Collection<IMetaObject> collection;

    public SortedListMetaObject(Collection<IMetaObject> fromCollection){
        collection = new ArrayList<>(fromCollection);
        calculateImoCrossDependencies();
    }

    private void calculateImoCrossDependencies(){
        Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());

        for(DBGitMetaType metaType : Sets.newHashSet(DBGitMetaType.DBGitTable, DBGitMetaType.DbGitFunction)){

            List<IMetaObject> objectsOfType = collection.stream().filter(x->x.getType().equals(metaType) ).collect(Collectors.toList());
            Map<String, String> realNamesToMetaNames = objectsOfType.stream().collect(Collectors.toMap(
                    x->x.getUnderlyingDbObject().getSchema() + "." + x.getUnderlyingDbObject().getName(),
                    IMetaObject::getName
                )
            );

            for(IMetaObject imo : objectsOfType){
                if(imo.getType().equals(DBGitMetaType.DbGitFunction)){
                    DBSQLObject dbsql = (DBSQLObject) imo.getUnderlyingDbObject();
                    Set<String> deps = realNamesToMetaNames.keySet().stream()
                            .filter( x -> dbsql.getSql().contains(x) && !(dbsql.getSchema()+"."+dbsql.getName()).equals(x) )
                            .map(realNamesToMetaNames::get)
                            .collect(Collectors.toSet());
                    dbsql.setDependencies(deps);
                }
                if(imo.getType().equals(DBGitMetaType.DBGitTable)){
                    DBTable dbTable = (DBTable) imo.getUnderlyingDbObject();
                    Set<String> deps = realNamesToMetaNames.values().stream()
                            .filter( x -> dbTable.getDependencies().contains(x) && !x.equals(imo.getName())  )
                            .collect(Collectors.toSet());
                    dbTable.setDependencies(deps);
                }
            }

        }

        Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
        Long diff = timestampAfter.getTime() - timestampBefore.getTime();
        ConsoleWriter.detailsPrintlnGreen(DBGitLang.getInstance().getValue("general", "time").withParams(diff.toString()));
    };

    public List<IMetaObject> sortFromDependant() throws ExceptionDBGit {
        if (listFromDependant == null) {
            listFromDependant = new ArrayList<>();
            List<DBGitMetaType> types = Arrays
                .stream(DBGitMetaType.values())
                .sorted(Comparator.comparing(DBGitMetaType::getPriority).reversed())
                .collect(Collectors.toList());

            for (DBGitMetaType tp : types) {

                List<IMetaObject> objectsOfType = collection.stream().filter(x -> x.getType().equals(tp)).collect(Collectors.toList());
                if (!objectsOfType.isEmpty()) {
                    if (tp.equals(DBGitMetaType.DBGitTable) || (objectsOfType.get(0) instanceof MetaSql)) {

                        Set<String> namesAllOfType = objectsOfType.stream().map(IMetaObject::getName).collect(Collectors.toSet());
                        List<IMetaObject> objectsL0 = objectsOfType.stream()
                                .filter(x -> x.getUnderlyingDbObject().getDependencies().size() == 0)
                                .collect(Collectors.toList());

                        objectsOfType.removeAll(objectsL0);
                        while (!objectsOfType.isEmpty()) {
                            Set<String> namesL0 = objectsL0.stream().map(IMetaObject::getName).collect(Collectors.toSet());
                            List<IMetaObject> objectsL1 = objectsOfType
                                    .stream()
                                    .filter(x -> {
                                        Set<String> actualDeps = new HashSet<>(x.getUnderlyingDbObject().getDependencies());
                                        actualDeps.retainAll(namesAllOfType);
                                        return namesL0.containsAll(actualDeps);
                                    })
                                    .sorted(imoDependenceComparator.reversed())
                                    .collect(Collectors.toList());
                            if (objectsL1.isEmpty()) {
                                warnNotAdded(objectsOfType);
                                throw new ExceptionDBGit("infinite loop");
                            }
                            objectsOfType.removeAll(objectsL1);
                            objectsL0.addAll(0, objectsL1);
                        }
                        listFromDependant.addAll(objectsL0);
                    } else {
                        listFromDependant.addAll(objectsOfType);
                    }
                }
            }

        }
        return listFromDependant;

    };
    public List<IMetaObject> sortFromFree() throws ExceptionDBGit {
        if (listFromFree == null) {
            listFromFree = new ArrayList<>();
            List<DBGitMetaType> types = Arrays.stream(DBGitMetaType.values())
                    .sorted(Comparator.comparing(DBGitMetaType::getPriority))
                    .collect(Collectors.toList());

            for (DBGitMetaType tp : types) {
                List<IMetaObject> objectsOfType = collection.stream().filter(x -> x.getType().equals(tp)).collect(Collectors.toList());
                if (!objectsOfType.isEmpty()) {
                    if (tp.equals(DBGitMetaType.DBGitTable) || objectsOfType.get(0) instanceof MetaSql) {
                        Set<String> namesAllOfType = objectsOfType.stream().map(IMetaObject::getName).collect(Collectors.toSet());
                        List<IMetaObject> objectsL0 = objectsOfType.stream().filter(x -> x.getUnderlyingDbObject().getDependencies().size() == 0).collect(Collectors.toList());

                        objectsOfType.removeAll(objectsL0);
                        while (!objectsOfType.isEmpty()) {
                            Set<String> namesL0 = objectsL0.stream().map(IMetaObject::getName).collect(Collectors.toSet());
                            List<IMetaObject> objectsL1 = objectsOfType
                                    .stream()
                                    .filter(x -> {
                                        Set<String> actualDeps = new HashSet<>(x.getUnderlyingDbObject().getDependencies());
                                        actualDeps.retainAll(namesAllOfType);
                                        return namesL0.containsAll(actualDeps);
                                    })
                                    .sorted(imoDependenceComparator)
                                    .collect(Collectors.toList());
                            if (objectsL1.isEmpty()) {
                                warnNotAdded(objectsOfType);
                                throw new ExceptionDBGit("infinite loop");
                            }
                            objectsOfType.removeAll(objectsL1);
                            objectsL0.addAll(objectsL1);
                        }
                        listFromFree.addAll(objectsL0);
                    } else {
                        listFromFree.addAll(objectsOfType);
                    }
                }

            }
        }
        return listFromFree;
    };

    public static Comparator<IMetaObject> imoTypeComparator = Comparator.comparing(x->x.getType().getPriority());
    public static Comparator<IMetaObject> imoDependenceComparator = (o1, o2) -> {

        int result = imoTypeComparator.compare(o1, o2);
        if( result == 0){
            if(o1 instanceof MetaSql || o1 instanceof MetaTable){
                Set<String> leftDeps = o1.getUnderlyingDbObject().getDependencies();
                Set<String> rightDeps = o2.getUnderlyingDbObject().getDependencies();

                if (rightDeps.contains(o1.getName())) result = -1;
                if (leftDeps.contains(o2.getName())) result = 1;
                if (rightDeps.size()!=0 && leftDeps.size()==0) result = -1;
                if (rightDeps.size()==0 && leftDeps.size()!=0) result = 1;
            }
            // dependant comes earlier than dependency
        }
        return result;
    };

    public void warnNotAdded(List<IMetaObject> remained){
        ConsoleWriter.detailsPrintlnRed("There were objects with unsatisfied dependencies, " +
                "they will NOT be included in restore list!\n");
        remained.forEach( x -> ConsoleWriter.printlnColor(x.getName(), Ansi.FColor.MAGENTA, 1) );
    }

}
