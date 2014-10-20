package org.distributedexternalsort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;

import com.google.code.externalsorting.BinaryFileBuffer;

/**
 * This class merges the sorted pieces of the input file.
 * 
 * @author Aniket Kokate
 */
public class MergeFiles {

	/**
	 * @param args
	 */
	/*public static void main(String[] args) {
		// TODO Auto-generated method stub

	}*/

	/**
	 * This merges a bunch of temporary flat files
	 * 
	 * @param files
	 *                The {@link List} of sorted {@link File}s to be merged.
	 * @param distinct
	 *                Pass <code>true</code> if duplicate lines should be
	 *                discarded. (elchetz@gmail.com)
	 * @param outputfile
	 *                The output {@link File} to merge the results to.
	 * @param cmp
	 *                The {@link Comparator} to use to compare
	 *                {@link String}s.
	 * @param cs
	 *                The {@link Charset} to be used for the byte to
	 *                character conversion.
	 * @param append
	 *                Pass <code>true</code> if result should append to
	 *                {@link File} instead of overwrite. Default to be false
	 *                for overloading methods.
	 * @param usegzip
	 *                assumes we used gzip compression for temporary files
	 * @return The number of lines sorted. (P. Beaudoin)
	 * @throws IOException
	 * @since v0.1.4
	 */
	public static int mergeSortedFiles(List<File> files, File outputfile,
			final Comparator<String> cmp, Charset cs, boolean distinct,
			boolean append, boolean usegzip) throws IOException {
		long startTime = System.currentTimeMillis();
		System.out.println("MergeFiles: Started");
		ArrayList<BinaryFileBuffer> bfbs = new ArrayList<BinaryFileBuffer>();
		for (File f : files) {
			final int BUFFERSIZE = 2048;
			InputStream in = new FileInputStream(f);
			BufferedReader br;
			if (usegzip) {
				br = new BufferedReader(
						new InputStreamReader(
								new GZIPInputStream(in,
										BUFFERSIZE), cs));
			} else {
				br = new BufferedReader(new InputStreamReader(
						in, cs));
			}

			BinaryFileBuffer bfb = new BinaryFileBuffer(br);
			bfbs.add(bfb);
		}
		BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(outputfile, append), cs));
		int rowcounter = mergeSortedFiles(fbw, cmp, distinct, bfbs);
		for (File f : files)
			f.delete();
		System.out.println("MergeFiles: Completed");
		System.out.println("MergeFiles: Total time: "+(System.currentTimeMillis() - startTime));
		return rowcounter;
	}

	/**
	 * This merges several BinaryFileBuffer to an output writer.
	 * 
	 * @param fbw
	 *                A buffer where we write the data.
	 * @param cmp
	 *                A comparator object that tells us how to sort the
	 *                lines.
	 * @param distinct
	 *                Pass <code>true</code> if duplicate lines should be
	 *                discarded. (elchetz@gmail.com)
	 * @param buffers
	 *                Where the data should be read.
	 * @return The number of lines sorted. (P. Beaudoin)
	 * @throws IOException
	 * 
	 */
	public static int mergeSortedFiles(BufferedWriter fbw,
			final Comparator<String> cmp, boolean distinct,
			List<BinaryFileBuffer> buffers) throws IOException {
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
				11, new Comparator<BinaryFileBuffer>() {
					//@Override
					public int compare(BinaryFileBuffer i,
							BinaryFileBuffer j) {
						return cmp.compare(i.peek(), j.peek());
					}
				});
		for (BinaryFileBuffer bfb : buffers)
			if (!bfb.empty())
				pq.add(bfb);
		int rowcounter = 0;
		String lastLine = null;
		try {
			while (pq.size() > 0) {
				BinaryFileBuffer bfb = pq.poll();
				String r = bfb.pop();
				// Skip duplicate lines
				if (!distinct || !r.equals(lastLine)) {
					fbw.write(r);
					fbw.newLine();
					lastLine = r;
				}
				++rowcounter;
				if (bfb.empty()) {
					bfb.fbr.close();
				} else {
					pq.add(bfb); // add it back
				}
			}
		} finally {
			fbw.close();
			for (BinaryFileBuffer bfb : pq)
				bfb.close();
		}
		return rowcounter;

	}

}
