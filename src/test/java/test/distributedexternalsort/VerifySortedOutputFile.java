package test.distributedexternalsort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * This a test class to verify the sorted output file.
 * 
 * @author Aniket Kokate
 *
 */
public class VerifySortedOutputFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args==null || args.length!=1) {
			System.out.println("Usage: java test.distributedexternalsort.VerifySortedOutputFile outputfile");
			return;
		}

		File f = new File(args[0]);
		BufferedReader reader = new BufferedReader(new FileReader(f));
		String lastLine = "";
		String currentLine;
		while((currentLine = reader.readLine())!=null) {
			if(lastLine.compareTo(currentLine)>0) {
				System.out.println("CurrentLine="+currentLine);
				System.out.println("LastLine="+lastLine);
				System.out.println("Sorting failed");
				break;
			}
			lastLine = currentLine;
		}
		reader.close();
		if(currentLine == null) {
			System.out.println("Sorting successful");
		}
	}

}
