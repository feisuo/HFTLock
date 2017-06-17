package ht.lock;

import java.util.Iterator;

public class ParsedPath {
	
	String path;
	boolean root;
	String[] slice;
	
	public boolean isRoot() {
		return root;
	}
	
	public String path() {
		return path;
	}
	
	public class DefaultIterator implements Iterator<String> {
		int index = 0;
		
		public boolean hasNext() {
			if (slice == null || index >= slice.length)
				return false;
			return true;
		}
		
		public String next() {
			return slice[index++];
		}
	}
	
	public Iterator<String> iterator() {
		return new DefaultIterator();
	}
	
	public class Cursor {
		int index;
		
		public boolean isValid() {
			return (index >= 0 && index < slice.length) ? true : false;
		}
		
		public String get() {
			return slice[index];
		}
		
		public boolean isLast() {
			return index == slice.length-1;
		}
		
		public void moveForward() {
			index++;
		}
		
		public void moveBack() {
			index--;
		}
	}
	
	public Cursor cursor() {
		return new Cursor();
	}
	
	public static ParsedPath parse(String path0) throws Exception {
		if (!path0.startsWith("/"))
			throw new IllegalArgumentException("path should starts with /");
		
		if (path0.equals("/")) {
			ParsedPath pp = new ParsedPath();
			pp.path = path0;
			pp.root = true;
			pp.slice = new String[]{"/"};
			return pp;
		}
		
		String path = rstrip(path0, '/');
		if (path.length() == 0)
			throw new IllegalArgumentException("path is invalid 1");
		
		String[] pathSlice = path.split("/");
		
		assert(pathSlice.length >= 2);
		
		pathSlice[0] = "/";
		for (String s : pathSlice) {
			if (s.length() == 0) {
				throw new IllegalArgumentException("path is invalid 2");
			}
		}
		
		
		ParsedPath pp = new ParsedPath();
		pp.path = path;
		pp.root = false;
		pp.slice = pathSlice;
		
		return pp;
	}
	
	public static String rstrip(String s, char c) {
		if (s == null || s.equals(""))
			return s;
		
		int idx = s.length() - 1;
		if (s.charAt(idx) == c)
			idx--;
		
		if (idx == s.length() - 1)
			return s;
		
		return s.substring(0, idx+1);
	}
	
	public boolean verify(String path) throws Exception {
		parse(path);
		return true;
	}
}
