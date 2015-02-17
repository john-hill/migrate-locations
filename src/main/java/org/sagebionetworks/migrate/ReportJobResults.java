package org.sagebionetworks.migrate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.AsyncLocationableTypeConversionResults;
import org.sagebionetworks.repo.model.LocationableTypeConversionResult;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.tool.progress.BasicProgress;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * Generated a CSV report that includes all results for each JobId provided.
 * 
 * @author jhill
 *
 */
public class ReportJobResults {
	
	private static final String NEW_TYPE = "new_type";
	private static final String ERROR = "error";
	private static final String CREATED_BY = "created_by";
	private static final String ORIGINAL_TYPE = "original_type";
	private static final String ENTITY_ID = "entity_id";
	private static final String SUCCESS = "success";

	public static String[] RESULT_HEADER = new String[]{ENTITY_ID, SUCCESS, ORIGINAL_TYPE, NEW_TYPE,CREATED_BY, ERROR};
	
	private SynapseClient synapse;
	
	public ReportJobResults(SynapseClient synapse, File jobFile) throws IOException, InterruptedException{
		if(!jobFile.exists()){
			throw new IllegalArgumentException("Job IDs files does not exist: "+jobFile.getAbsolutePath());
		}
		this.synapse = synapse;
		
		LinkedHashSet<String> jobIds = readJobIds(jobFile);
		
		File temp = File.createTempFile("LocationMigrationResults", ".csv");
		System.out.println("Writting results to: "+temp.getAbsolutePath());
		// Get the results for each job
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(temp), "UTF-8"));
		try{
			// setup the header.
			writer.writeNext(RESULT_HEADER);
			// Write each job's results
			for(String jobId: jobIds){
				System.out.println("Fetching job: "+jobId);
				List<LocationableTypeConversionResult> result = fetchJobResutls(jobId);
				writeResults(result, writer);
			}
			writer.flush();
		}finally{
			writer.close();
		}
		System.out.println("Finished file: "+temp.getAbsolutePath());

	}
	
	/**
	 * Read the JobIds from the file.
	 * @param jobIdFile
	 * @return
	 * @throws IOException
	 */
	private LinkedHashSet<String> readJobIds(File jobIdFile) throws IOException{
		LinkedHashSet<String> linked = new LinkedHashSet<String>();
		CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(jobIdFile)));
		try{
			String[] array = null;
			do{
				array = reader.readNext();
				if(array != null){
					linked.add(array[0]);
				}
			}while(array != null);
			return linked;
		}finally{
			reader.close();
		}
	}
	
	/**
	 * Fetch a job's results.
	 * @param jobId
	 * @return
	 * @throws InterruptedException
	 */
	private List<LocationableTypeConversionResult> fetchJobResutls(String jobId) throws InterruptedException{
		BasicProgress progress = new BasicProgress();
		progress.setMessage("Waiting for job: "+jobId);
		// Wait for the job
		while(true){
			try {
				AsyncLocationableTypeConversionResults results = synapse.getLocationableTypeConverJobResults(jobId);
				return results.getResults();
			} catch(SynapseResultNotReadyException e){
				AsynchronousJobStatus status = e.getJobStatus();
				if(AsynchJobState.FAILED.equals(status.getJobState())){
					System.out.println("Job failed: "+jobId+" message: "+status.getErrorMessage());
					return new LinkedList<LocationableTypeConversionResult>();
				}
				if(status.getProgressCurrent() != null){
					progress.setCurrent(status.getProgressCurrent());
				}
				if(status.getProgressTotal() != null){
					progress.setTotal(status.getProgressTotal());
				}
			} catch (SynapseException e) {
				// Something else went wrong.
				e.printStackTrace();
				return new LinkedList<LocationableTypeConversionResult>();
			}
			System.out.println(progress.getCurrentStatus().toString());
			Thread.sleep(1000);
		}
	}
	
	/**
	 * Write all of the resutls to the CSV
	 * @param results
	 * @param writer
	 * @throws IOException
	 */
	private void writeResults(List<LocationableTypeConversionResult> results, CSVWriter writer) throws IOException{
		String[] array = new String[RESULT_HEADER.length];
		for(LocationableTypeConversionResult result: results){
			if(result.getSuccess() == null){
				result.setSuccess(false);
			}
			set(array, ENTITY_ID, result.getEntityId());
			set(array, SUCCESS, result.getSuccess().toString());
			set(array, ORIGINAL_TYPE, result.getOriginalType());
			set(array, NEW_TYPE, result.getNewType());
			set(array, CREATED_BY, result.getCreatedBy());
			set(array, ERROR, result.getErrorMessage());
			writer.writeNext(array);
			writer.flush();
		}
	}
	
	/**
	 * Set the value in the array
	 * @param array
	 * @param name
	 * @param value
	 */
	private void set(String[] array, String name, String value){
		array[index(name)] = value;
	}
	
	/**
	 * Lookup the index of the header.
	 * @param name
	 * @return
	 */
	private int index(String name){
		for(int i=0; i<RESULT_HEADER.length; i++){
			if(RESULT_HEADER[i].equals(name)){
				return i;
			}
		}
		throw new IllegalArgumentException("Unknown name:"+name);
	}

	public static void main(String[] args) throws Exception {
		SynapseClient synapse = SynapseClientUtils.createSynapseConnection(args);
		if(args == null || args.length < 6){
			throw new IllegalArgumentException("args[5]=JobIdsFile");
		}
		File jobIds = new File(args[5]);
		new ReportJobResults(synapse, jobIds);
	}

}
