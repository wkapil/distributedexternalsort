/*
 * JPPF.
 * Copyright (C) 2005-2013 JPPF Team.
 * http://www.jppf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.distributedexternalsort;

import java.util.ArrayList;
import java.util.List;

import org.jppf.client.JPPFClient;
import org.jppf.client.JPPFJob;
import org.jppf.client.JPPFResultCollector;
import org.jppf.server.protocol.JPPFTask;

/**
 * This is a JPPF application runner.
 * It will create tasks per piece of the input file and sort those on individual nodes.
 * 
 * @author Aniket Kokate
 */
public class SortJPPFRunner {
	/**
	 * The JPPF client, handles all communications with the server.
	 * It is recommended to only use one JPPF client per JVM, so it
	 * should generally be created and used as a singleton.
	 */
	private static JPPFClient jppfClient =  null;
	private static List<String> resultList = null;

	public static List<String> sortFiles(String[] fileNames) {
		try {
			System.out.println("SortJPPFRunner: Started");

			long startDate = System.currentTimeMillis();

			// create the JPPFClient. This constructor call causes JPPF to read the configuration file
			// and connect with one or multiple JPPF drivers.
			jppfClient = new MyJPPFClient();

			// Create a job
			JPPFJob job = createJob();
			System.out.println("SortJPPFRunner: Job created");

			System.out.println("SortJPPFRunner: Files to sort:"+fileNames.length);
			for(int i=0; i<fileNames.length; i++) {
				System.out.println("SortJPPFRunner: File["+i+"]: "+fileNames[i]);
				SortJPPFTask s = (SortJPPFTask) job.addTask(new SortJPPFTask(fileNames[i]));
				s.setName("SortJPPFTask "+i);
				s.setId("SortJPPFTask "+i);
			}

			//job.getSLA().setSuspended(true);

			// execute a blocking job
			executeBlockingJob(job);

			System.out.println("SortJPPFRunner: ---------Output:"+resultList);

			// execute a non-blocking job
			//runner.executeNonBlockingJob(job);

			long endDate = System.currentTimeMillis();
			System.out.println("SortJPPFRunner: Completed");
			System.out.println("SortJPPFRunner: Total Time (ms): "+(endDate-startDate));
		} catch(Exception e) {
			System.out.println("SortJPPFRunner: Exception");
			e.printStackTrace();
		} finally {
			if (jppfClient != null) jppfClient.close();
		}
		return resultList;
	}

	/**
	 * The entry point for this application runner to be run from a Java command line.
	 * @param args by default, we do not use the command line arguments,
	 * however nothing prevents us from using them if need be.
	 */
	/*public static void main(final String...args) {
		SortJPPFRunner runner = new SortJPPFRunner(null);
	}*/

	/**
	 * Create a JPPF job that can be submitted for execution.
	 * @return an instance of the {@link org.jppf.client.JPPFJob JPPFJob} class.
	 * @throws Exception if an error occurs while creating the job or adding tasks.
	 */
	private static JPPFJob createJob() throws Exception {

		// create a JPPF job
		JPPFJob job = new JPPFJob();

		// give this job a readable unique id that we can use to monitor and manage it.
		job.setName("DistributedExternalSort");

		return job;
	}

	/**
	 * Execute a job in blocking mode. The application will be blocked until the job
	 * execution is complete.
	 * @param job the JPPF job to execute.
	 * @throws Exception if an error occurs while executing the job.
	 */
	private static void executeBlockingJob(final JPPFJob job) throws Exception {
		// set the job in blocking mode.
		job.setBlocking(true);

		// Submit the job and wait until the results are returned.
		// The results are returned as a list of JPPFTask instances,
		// in the same order as the one in which the tasks where initially added the job.
		List<JPPFTask> results = jppfClient.submit(job);

		// process the results
		processExecutionResults(results);
	}

	/**
	 * Execute a job in non-blocking mode. The application has the responsibility
	 * for handling the notification of job completion and collecting the results.
	 * @param job the JPPF job to execute.
	 * @throws Exception if an error occurs while executing the job.
	 */
	private static void executeNonBlockingJob(final JPPFJob job) throws Exception {
		// set the job in non-blocking (or asynchronous) mode.
		job.setBlocking(false);

		// this call returns immediately. We will use the collector at a later time
		// to obtain the execution results asynchronously
		JPPFResultCollector collector = submitNonBlockingJob(job);

		// the non-blocking job execution is asynchronous, we can do anything else in the meantime
		System.out.println("Doing something while the job is executing ...");
		// ...

		// We are now ready to get the results of the job execution.
		// We use JPPFResultCollector.waitForResults() for this. This method returns immediately with
		// the results if the job has completed, otherwise it waits until the job execution is complete.
		List<JPPFTask> results = collector.waitForResults();

		// process the results
		processExecutionResults(results);
	}

	/**
	 * Execute a job in non-blocking mode. The application has the responsibility
	 * for handling the notification of job completion and collecting the results.
	 * @param job the JPPF job to execute.
	 * @return a JPPFResultCollector used to obtain the execution results at a later time.
	 * @throws Exception if an error occurs while executing the job.
	 */
	private static JPPFResultCollector submitNonBlockingJob(final JPPFJob job) throws Exception {
		// set the job in non-blocking (or asynchronous) mode.
		job.setBlocking(false);

		// We need to be notified of when the job execution has completed.
		// To this effect, we define an instance of the TaskResultListener interface,
		// which we will register with the job.
		// Here, we use an instance of JPPFResultCollector, conveniently provided by the JPPF API.
		// JPPFResultCollector implements TaskResultListener and has a constructor that takes
		// the number of tasks in the job as a parameter.
		JPPFResultCollector collector = new JPPFResultCollector(job);
		job.setResultListener(collector);

		// Submit the job. This call returns immediately without waiting for the execution of
		// the job to complete. As a consequence, the object returned for a non-blocking job is
		// always null. Note that we are calling the exact same method as in the blocking case.
		jppfClient.submit(job);

		// finally return the result collector, so it can be used to collect the exeuction results
		// at a time of our chosing. The collector can also be obtained at any time by calling 
		// (JPPFResultCollector) job.getResultListener()
		return collector;
	}

	/**
	 * Process the execution results of each submitted task. 
	 * @param results the tasks results after execution on the grid.
	 */
	private static void processExecutionResults(final List<JPPFTask> results) {
		resultList = new ArrayList<String>();
		// process the results
		for (JPPFTask task: results) {
			// if the task execution resulted in an exception
			if (task.getException() != null) {
				// process the exception here ...
				task.getException().printStackTrace();
				System.out.println("Task Id:"+task.getId()+": An exception was raised: " + task.getException().getMessage());
			}
			else {
				// process the result here ...
				System.out.println("Task Id:"+task.getId()+": Execution result: " + task.getResult());
				if(task.getResult() != null)
					resultList.add((String) task.getResult());
			}
		}
	}


}
