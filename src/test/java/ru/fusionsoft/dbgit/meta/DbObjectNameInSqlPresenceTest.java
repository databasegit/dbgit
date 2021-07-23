package ru.fusionsoft.dbgit.meta;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class DbObjectNameInSqlPresenceTest {

    @Test
    public final void matchesName() {
        final DbObjectNameInSqlPresence presentCase = new DbObjectNameInSqlPresence(
            "inventory_in_stock",
            "CREATE OR REPLACE FUNCTION public.film_in_stock(p_film_id integer, p_store_id integer, OUT p_film_count integer)\n"
            + "RETURNS SETOF integer\n"
            + "LANGUAGE sql\n"
            + " AS $function$\n"
            + "      SELECT inventory_id\n"
            + "      FROM inventory\n"
            + "      WHERE film_id = $1\n"
            + "      AND store_id = $2\n"
            + "      AND inventory_in_stock(inventory_id);\n"
            + " $function$"
        );
        
        assertTrue(presentCase.matches());
    }
    @Test
    public final void notMatchesSqlWithNameUnderscoped() {

        final DbObjectNameInSqlPresence notPresentCase1 = new DbObjectNameInSqlPresence(
            "group_concat",
            "... (\n"
            + "    SFUNC = _group_concat,\n"
            + "    STYPE = text\n"
            + ");"
        );
       
        assertFalse(notPresentCase1.matches());
    }
    
    @Test
    public final void notMatchesSqlWithSchemaAndUderscopedName() {
        final DbObjectNameInSqlPresence notPresentCase2 = new DbObjectNameInSqlPresence(
            "group_concat",
            "CREATE OR REPLACE FUNCTION public._group_concat(text, text)\n"
            + "   RETURNS text\n"
            + "   LANGUAGE sql\n"
            + "   IMMUTABLE\n"
            + "AS $function$\n"
            + "  SELECT CASE\n"
            + "    WHEN $2 IS NULL THEN $1\n"
            + "    WHEN $1 IS NULL THEN $2\n"
            + "    ELSE $1 || ', ' || $2\n"
            + "  END\n"
            + "$function$"
        );

        assertFalse(notPresentCase2.matches());

    }    
    @Test
    public final void matchesSqlWithSchemaAndUderscopedName() {
        final DbObjectNameInSqlPresence presentCase2 = new DbObjectNameInSqlPresence(
            "_group_concat",
            "CREATE OR REPLACE FUNCTION public._group_concat(text, text)\n"
            + "   RETURNS text\n"
            + "   LANGUAGE sql\n"
            + "   IMMUTABLE\n"
            + "AS $function$\n"
            + "  SELECT CASE\n"
            + "    WHEN $2 IS NULL THEN $1\n"
            + "    WHEN $1 IS NULL THEN $2\n"
            + "    ELSE $1 || ', ' || $2\n"
            + "  END\n"
            + "$function$"
        );

        assertTrue(presentCase2.matches());

    }

}

