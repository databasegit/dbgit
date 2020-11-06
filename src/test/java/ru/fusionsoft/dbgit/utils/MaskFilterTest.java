package ru.fusionsoft.dbgit.utils;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class MaskFilterTest {

    @Test
    public void match() {
        MaskFilter mf = new MaskFilter("pub*.*");

        String textTbl = "public/ad_group_roles.tbl";
        String textCsv = "public/ad_group_roles.csv";

        assertTrue(mf.match(textTbl));
        assertTrue(mf.match(textCsv));
    }

    @Test
    public void matchExtention() {
        MaskFilter mfCsv = new MaskFilter("pub*lic*.csv");

        String textTbl = "public/ad_group_roles.tbl";
        String textCsv = "public/ad_group_roles.csv";

        assertFalse(mfCsv.match(textTbl));
        assertTrue(mfCsv.match(textCsv));
    }
}