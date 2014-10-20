package org.distributedexternalsort;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * This is a simple class to split the input file line count wise.
 * 
 * @author Aniket Kokate
 *
 */
public class SplitFile {

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws NumberFormatException 
	 *//*
	public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {
		String[] fileNames = splitFileByLines(args[0], Integer.parseInt(args[1]), args[2], args[3]);
		System.out.println("File list:");
		for(String fileName: fileNames) {
			System.out.println(fileName);
		}
	}*/

	public static String[] splitFileByLines(String fileName, long numberOfLines, String outputDirectory, final String prefix) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		System.out.println("SplitFile: started");
		String[] fileNames = null;
		String splitFileCommand = "split -l "+numberOfLines+" "+fileName+" "+outputDirectory+"/"+prefix+" --numeric-suffixes";
		System.out.println("Executing: "+splitFileCommand);
		Process p = Runtime.getRuntime().exec(splitFileCommand);
		p.waitFor();
		System.out.println("split p.exitValue()="+p.exitValue());
		if(p.exitValue()==0) {
			File dir = new File(outputDirectory);
			fileNames = dir.list(new FilenameFilter() {
				//@Override
			    public boolean accept(File dir, String name) {
			        return name.startsWith(prefix);
			    }
			});
			System.out.println("SplitFile: split fileNames length="+fileNames.length);
		}
		System.out.println("SplitFile: completed");
		System.out.println("SplitFile: Total time: "+(System.currentTimeMillis() - startTime));
		return fileNames;
	}

}
