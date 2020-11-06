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
    String textTbl = "public/ad_group_roles.tbl";

    private void createDbGitIgnore(){
        dbGitIgnore = new DBGitIgnore(filters, exclusions);
        addExcl("public/*.*");
    }

    private void addFilter(String mask){ filters.put(mask, new MaskFilter(mask)); }
    private void addExcl(String mask){ exclusions.put(mask, new MaskFilter(mask)); }



    @Test
    public void matchOne() {
        createDbGitIgnore();
        assertFalse(dbGitIgnore.matchOne(textTbl));
    }
}