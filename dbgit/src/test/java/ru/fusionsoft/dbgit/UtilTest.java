package ru.fusionsoft.dbgit;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import ru.fusionsoft.dbgit.utils.MaskFilter;

public class UtilTest extends TestCase {
	public void testMask() {
		MaskFilter mask = new MaskFilter("asd*.txt");
		assertTrue(mask.match("asd12df.txt"));
		assertFalse(mask.match("as612df.txt"));
		
		mask = new MaskFilter("path/asd*.txt");
		assertTrue(mask.match("path/asd12df.txt"));		
		assertFalse(mask.match("as612df.txt"));
		
		mask = new MaskFilter("path/*");
		assertTrue(mask.match("path/asd12df.txt"));		
		assertFalse(mask.match("pat/as612df.txt"));
	
		mask = new MaskFilter("path/w*");
		assertTrue(mask.match("path\\wsd12df.txt"));		
		assertFalse(mask.match("pat\\ws612df.txt"));
	}
}
