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

import org.jppf.server.protocol.JPPFTask;

//import com.google.code.externalsorting.ExternalSort;
import com.google.code.externalsorting.ExternalSortCallable;
import com.google.code.externalsorting.ExternalSortParallelStreamSort;

/**
 * This class is a JPPF task.
 * There are 3 parts to a task that is to be executed on a JPPF grid:
 * <ol>
 * <li>the task initialization: this is done on the client side, generally from the task constructor,
 * or via explicit method calls on the task from the application runner.</li>
 * <li>the task execution: this part is performed by the node. It consists in invoking the {@link #run() run()} method,
 * and handling an eventual uncaught {@link java.lang.Throwable Throwable} that would result from this invocation.</li>
 * <li>getting the execution results: the task itself, after its execution, is considered as the result.
 * JPPF provides the convenience methods {@link org.jppf.server.protocol.JPPFTask#setResult(java.lang.Object) setResult(Object)} and
 * {@link org.jppf.server.protocol.JPPFTask#getResult() getResult()}
 * to this effect, however any accessible attribute of the task will be available when the task is returned to the client.</li>
 * </ol>
 * @author Aniket Kokate
 */
public class SortJPPFTask extends JPPFTask
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6470084032188854957L;

	private String name;
	String fileName;
	/**
	 * Perform initializations on the client side,
	 * before the task is executed by the node.
	 */
	public SortJPPFTask(String fileName)
	{
		// perform initializations here ...
		this.fileName = fileName;
	}

	/**
	 * This method contains the code that will be executed by a node.
	 * Any uncaught {@link java.lang.Throwable Throwable} will be handled as follows:
	 * <ul>
	 * <li>if the {@link java.lang.Throwable Throwable} is an instance of {@link java.lang.Exception Exception},
	 * it will be stored in the task via a call to {@link org.jppf.server.protocol.JPPFTask#setException(java.lang.Exception) JPPFTask.setException(Exception)}</li>
	 * <li>otherwise, it will first be wrapped in a {@link org.jppf.JPPFException JPPFException},
	 * then this <code>JPPFException</code> will be stored in the task via a call to {@link org.jppf.server.protocol.JPPFTask#setException(java.lang.Exception) JPPFTask.setException(Exception)}</li>
	 * </ul>
	 * @see java.lang.Runnable#run()
	 */
	//@Override
	public void run()
	{
		String outputFileName = fileName + "_sorted";
		try {
			long startTime = System.currentTimeMillis();
			// write your task code here.
			System.out.println("SortJPPFTask: Executing - "+name);

			String[] param = new String[2];
			param[0] = fileName;
			param[1] = outputFileName;
			// Compatible upto Java 7: Without multi threading
			//ExternalSort.main(param);
			// Compatible Java 8: With multi threading and Executor Service
			ExternalSortCallable.main(param);
			// Compatible Java 8: Without multi threading but with Executor Service
			//ExternalSortParallelStreamSort.main(param);
			
			System.out.println("SortJPPFTask: "+name+" Total time: "+(System.currentTimeMillis()-startTime));
			System.out.println("SortJPPFTask: "+name+" ended...");

		} catch(Exception e){
			System.out.println("SortJPPFTask: "+name+": Exception occured..."+e.getMessage());
			e.printStackTrace();
			setException(e);
			return;
		}
		System.out.println("SortJPPFTask: "+name+" ended : "+name);

		// eventually set the execution results
		setResult(outputFileName);
		//setResult(name+":Done.");
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
