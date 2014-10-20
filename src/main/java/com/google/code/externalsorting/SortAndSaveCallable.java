package com.google.code.externalsorting;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;
//import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
//import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * This tread class will sort the provided temporary file.
 * 
 * @author Aniket Kokate
 * 
 */
public class SortAndSaveCallable implements Callable<File> {

	private List<String> tmplist;
	private Comparator<String> cmp;
	private Charset cs;
	private File tmpdirectory;
	private boolean distinct;
	private boolean usegzip;

	public SortAndSaveCallable(List<String> tmplist,
			Comparator<String> cmp, Charset cs, File tmpdirectory,
			boolean distinct, boolean usegzip) {
		this.tmplist = tmplist;
		this.cmp = cmp;
		this.cs = cs;
		this.tmpdirectory = tmpdirectory;
		this.distinct = distinct;
		this.usegzip = usegzip;
	}

	//@Override
	public File call() throws IOException {
		Collections.sort(tmplist, cmp);
		//String[] tmplistArray = tmplist.toArray(new String[0]);
		//Arrays.parallelSort(tmplistArray, cmp);
		/*tmplist = tmplist.parallelStream().sorted(cmp)        		
				.collect(Collectors.toCollection(ArrayList<String>::new));*/
		File newtmpfile = File.createTempFile("sortInBatch",
				"flatfile", tmpdirectory);
		newtmpfile.deleteOnExit();
		OutputStream out = new FileOutputStream(newtmpfile);
		//System.out.println("callable newtmpfile-rec size-"+tmplist.size()+"-"+newtmpfile.getPath());
		int ZIPBUFFERSIZE = 2048;
		if (usegzip)
			out = new GZIPOutputStream(out, ZIPBUFFERSIZE) {
			{
				this.def.setLevel(Deflater.BEST_SPEED);
			}
		};
		BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
				out, cs));
		String lastLine = null;
		try {
			//System.out.println("callable tmplist size="+tmplist.size());
			for (String r : tmplist) {
			//for (String r : tmplistArray) {
				// Skip duplicate lines
				if (!distinct || !r.equals(lastLine)) {
					fbw.write(r);
					fbw.newLine();
					lastLine = r;
				}
			}
			//System.out.println("callable processed tmplist size="+tmplist.size());
		} finally {
			fbw.close();
			tmplist.clear();
		}

		return newtmpfile;
	}

}
