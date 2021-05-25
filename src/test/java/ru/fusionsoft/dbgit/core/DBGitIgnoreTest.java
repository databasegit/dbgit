package ru.fusionsoft.dbgit.core;

import ru.fusionsoft.dbgit.utils.MaskFilter;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class DBGitIgnoreTest {

    DBGitIgnore dbGitIgnore;
    Map<String, MaskFilter> filters = new HashMap<>();
    Map<String, MaskFilter> exclusions = new HashMap<>();

    private void createDbGitIgnore(){
        dbGitIgnore = new DBGitIgnore(filters, exclusions);
    }

    private void addFilter(String mask){ filters.put(mask, new MaskFilter(mask)); }
    private void addExcl(String mask){ exclusions.put(mask, new MaskFilter(mask)); }



    @Test
    public void matchOne() {
        createDbGitIgnore();
        addExcl("public/*.*");
        
        final String textTbl = "public/ad_group_roles.tbl";
        assertFalse(dbGitIgnore.matchOne(textTbl));
    }
}
