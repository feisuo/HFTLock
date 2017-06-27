package ht.test.lock;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import com.tchaicatkovsky.lock.ParsedPath;

public class TestParsedPath {
	@Test
	public void test01() throws Exception {
		String path = "/ls/local/bigtable/master";
		ParsedPath pp = ParsedPath.parse(path);
		assertTrue(!pp.isRoot());
		String[] ary = new String[]{"/","ls","local","bigtable","master"};
		Iterator<String> it = pp.iterator();
		for (int i = 0; i < ary.length; i++) {
			assertTrue(it.hasNext());
			assertTrue(it.next().equals(ary[i]));
		}
	}
	
	@Test
	public void testRStripLogic() throws Exception {
		String path = "/ls/local/bigtable/master/";
		ParsedPath pp = ParsedPath.parse(path);
		assertTrue(!pp.isRoot());
		String[] ary = new String[]{"/","ls","local","bigtable","master"};
		Iterator<String> it = pp.iterator();
		for (int i = 0; i < ary.length; i++) {
			assertTrue(it.hasNext());
			assertTrue(it.next().equals(ary[i]));
		}
	}
	
	@Test
	public void testRoot() throws Exception {
		String path = "/";
		ParsedPath pp = ParsedPath.parse(path);
		assertTrue(pp.isRoot());
		String[] ary = new String[]{"/"};
		Iterator<String> it = pp.iterator();
		for (int i = 0; i < ary.length; i++) {
			assertTrue(it.hasNext());
			assertTrue(it.next().equals(ary[i]));
		}
	}
	
	@Test
	public void testError2_1() throws Exception {
		String path = "/ls/local//bigtable/master";
		Throwable t = null;
		try {
			ParsedPath.parse(path);
		} catch (Exception e) {
			t = e;
		}
		assertNotNull(t);
		assertTrue(t.getMessage().contains("path is invalid 2"));
	}
	
	@Test
	public void testError2_2() throws Exception {
		String path = "/ls/local/bigtable/master////";
		Throwable t = null;
		try {
			ParsedPath.parse(path);
		} catch (Exception e) {
			t = e;
		}
		assertNull(t);
	}
}
