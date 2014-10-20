package org.distributedexternalsort;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple class to count the number of lines in a file.
 * 
 * @author Aniket Kokate
 * 
 */
public class CountLines {

	/**
	 * @param args
	 *//*
	public static void main(String[] args) throws IOException {

		countLines(args[0]);

	}*/
	
	public static long countLines(String fileName) throws IOException {
		long startTime = System.currentTimeMillis();
		long count = 0;
		boolean empty = true;
		InputStream is = new BufferedInputStream(new FileInputStream(fileName));
		try {
			byte[] c = new byte[1024];
			int readChars = 0;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			System.out.println("CountLines: Count of lines: "+count);
		} finally {
			is.close();
		}
		System.out.println("CountLines: Total time: "+(System.currentTimeMillis() - startTime));
		return (count == 0 && !empty) ? 1 : count;
	}

}
