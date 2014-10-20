package org.distributedexternalsort;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Goal: offer a generic distributed external-memory sorting program in Java.
 * 
 * It must be : - hackable (easy to adapt) - scalable to large files - sensibly
 * efficient.
 * 
 * This software is in the public domain.
 * 
 * This software will count the number of lines of input file assuming a record per line.
 * Then split the input file into pieces and distribute the pieces into available JPPF nodes.
 * These pieces has to be stored at some shared location accessible to all of the JPPF nodes.
 * Each JPPF node will internally execute com/google/code/externalsorting/ExternalSort to sort
 * the allocated piece of file and store the sorted piece at the same shared location.
 * DistributedExternalSort will then merge the sorted pieces into a final sorted output file.
 * 
 * JPPF (Java Parallel Processing Framework): Please Refer http://www.jppf.org/
 * 
 * Usage: java org/distributedexternalsort/DistributedExternalSort somefile.txt out.txt sharedstore
 * 
 * You can change the default maximal number of temporary files with the -t
 * flag: java org/distributedexternalsort/DistributedExternalSort somefile.txt out.txt sharedstore
 * -t 3
 * This parameter will be forwarded to com/google/code/externalsorting/ExternalSort which creates temporary
 * files on individual nodes.
 * 
 * For very large files, you might want to use an appropriate flag to allocate
 * more memory to the Java VM: java -Xms2G
 * org/distributedexternalsort/DistributedExternalSort somefile.txt out.txt sharedstore
 * 
 * By Kapil Wadodkar, Aniket Kokate, First published: October 2014 originally posted at
 * https://github.com/wkapil/distributedexternalsort
 * We have used the External sorting mechanism provided by following team:
 * 
 * By (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon
 * Elsas, Christan Grant, Daniel Haran, Daniel Lemire, Sugumaran Harikrishnan,
 * Thomas, Mueller, Jerry Yang, First published: April 2010 originally posted at
 * http://lemire.me/blog/archives/2010/04/01/external-memory-sorting-in-java/
 */
public class DistributedExternalSort {

	/**
	 * 
	 */
	public static void displayUsage() {
		System.out
		.println("java org.distributedexternalsort.DistributedExternalSort inputfile outputfile sharedstore");
		System.out.println("Flags are:");
		System.out.println("-v or --verbose: verbose output");
		System.out.println("-d or --distinct: prune duplicate lines");
		System.out
		.println("-t or --maxtmpfiles (followed by an integer): specify an upper bound on the number of temporary files");
		System.out
		.println("-c or --charset (followed by a charset code): specify the character set to use (for sorting)");
		System.out
		.println("-z or --gzip: use compression for the temporary files");
		System.out
		.println("-H or --header (followed by an integer): ignore the first few lines");
		System.out
		.println("-s or --store (following by a path): where to store the temporary files on JPPF nodes");
		System.out.println("-h or --help: display this message");
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(final String[] args) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		boolean verbose = false;
		boolean distinct = false;
		int maxtmpfiles = DEFAULTMAXTEMPFILES;
		Charset cs = Charset.defaultCharset();
		String inputfile = null, outputfile = null, sharedstore = null;
		File tempFileStore = null;
		boolean usegzip = false;
		int headersize = 0;
		for (int param = 0; param < args.length; ++param) {
			if (args[param].equals("-v")
					|| args[param].equals("--verbose")) {
				verbose = true;
			} else if ((args[param].equals("-h") || args[param]
					.equals("--help"))) {
				displayUsage();
				return;
			} else if ((args[param].equals("-d") || args[param]
					.equals("--distinct"))) {
				distinct = true;
			} else if ((args[param].equals("-t") || args[param]
					.equals("--maxtmpfiles"))
					&& args.length > param + 1) {
				param++;
				maxtmpfiles = Integer.parseInt(args[param]);
				if (headersize < 0) {
					System.err
					.println("maxtmpfiles should be positive");
				}
			} else if ((args[param].equals("-c") || args[param]
					.equals("--charset"))
					&& args.length > param + 1) {
				param++;
				cs = Charset.forName(args[param]);
			} else if ((args[param].equals("-z") || args[param]
					.equals("--gzip"))) {
				usegzip = true;
			} else if ((args[param].equals("-H") || args[param]
					.equals("--header")) && args.length > param + 1) {
				param++;
				headersize = Integer.parseInt(args[param]);
				if (headersize < 0) {
					System.err
					.println("headersize should be positive");
				}
			} else if ((args[param].equals("-s") || args[param]
					.equals("--store")) && args.length > param + 1) {
				param++;
				tempFileStore = new File(args[param]);
			} else {
				if (inputfile == null)
					inputfile = args[param];
				else if (outputfile == null)
					outputfile = args[param];
				else if (sharedstore == null)
					sharedstore = args[param];
				else
					System.out.println("Unparsed: "
							+ args[param]);
			}
		}
		if (outputfile == null) {
			System.out
			.println("Please provide input and output file names");
			displayUsage();
			return;
		}
		if (sharedstore == null) {
			System.out
			.println("Please provide shared store location");
			displayUsage();
			return;
		}

		// Single task - Count lines of input file
		System.out.println("DistributedExternalSort: Counting lines -------------------------------");
		long inputFileLinesCount = CountLines.countLines(inputfile);

		// Single task - Calculate lines per file on basis of total number of lines and number of nodes
		int numberOfNodes = 4; // TODO: Call JPPF API to get available running node count
		long linesPerFile = inputFileLinesCount / numberOfNodes;

		// Single task - Split input file
		System.out.println("DistributedExternalSort: Splitting files -------------------------------");
		//String sharedstore = "/tmp";
		//String sharedstore = "//FileStore_IP/testdata";

		String[] fileNames = SplitFile.splitFileByLines(inputfile, linesPerFile, sharedstore, "ExSortSplit");
		for(int i=0; fileNames!=null && i<fileNames.length; i++) {
			fileNames[i] = sharedstore + "/" + fileNames[i];
			//fileNames[i] = sharedstore + "\\" + fileNames[i];
			System.out.println("DistributedExternalSort: Split file name["+i+"]: "+fileNames[i]);
		}

		System.out.println("DistributedExternalSort: Sorting split files -------------------------------");
		if(fileNames!=null && fileNames.length > 0) {
			// Distributed task - Call JPPF and sort the split files in distributed mode
			List<String> outputFileNames = SortJPPFRunner.sortFiles(fileNames);
			if(outputFileNames!=null && outputFileNames.size() > 0) {
				File f;
				List<File> fileList = new ArrayList<File>();
				for(String fileName: outputFileNames) {
					f = new File(fileName);
					f.setReadable(true);
					f.setWritable(true);
					f.setExecutable(true);
					fileList.add(f);
				}

				// Single task - Merge the sorted split files into output file
				System.out.println("DistributedExternalSort: Merging split files -------------------------------");
				MergeFiles.mergeSortedFiles(fileList, new File(outputfile), defaultcomparator, cs,
						distinct, false, usegzip);
				
			} else {
				System.out.println("Error: No files to merge.");
			}
		} else {
			System.out.println("Error: No split files found.");
		}
		
		// Single task - Delete the split files into output file
		File f;
		for(int i=0; fileNames!=null && i<fileNames.length; i++) {
			f = new File(fileNames[i]);
			f.delete();
			System.out.println("DistributedExternalSort: Deleted file name["+i+"]: "+fileNames[i]);
		}
		System.out.println("DistributedExternalSort: Total time (ms): "+ (System.currentTimeMillis() - startTime));
	}

	/**
	 * default comparator between strings.
	 */
	public static Comparator<String> defaultcomparator = new Comparator<String>() {
		//@Override
		public int compare(String r1, String r2) {
			return r1.compareTo(r2);
		}
	};

	/**
	 * Default maximal number of temporary files allowed.
	 */
	public static final int DEFAULTMAXTEMPFILES = 1024;

}
