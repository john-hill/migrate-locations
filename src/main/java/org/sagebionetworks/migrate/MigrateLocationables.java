package org.sagebionetworks.migrate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.AsyncLocationableTypeConversionRequest;
import org.sagebionetworks.repo.model.AsyncLocationableTypeConversionResults;
import org.sagebionetworks.repo.model.LocationableTypeConversionResult;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.tool.progress.AggregateProgress;
import org.sagebionetworks.tool.progress.BasicProgress;
import org.sagebionetworks.util.Pair;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class MigrateLocationables {
	
	/**
	 * Limit the total of entities per run.
	 * Set to infinity for all.
	 */
	private static final long MAX_ENTITES_PER_RUN = 1;
	
	private static final int BATCH_SIZE = 1000;
	private static final String NEW_TYPE = "new_type";
	private static final String ERROR = "error";
	private static final String CREATED_BY = "created_by";
	private static final String ORIGINAL_TYPE = "original_type";
	private static final String ENTITY_ID = "entity_id";
	private static final String SUCCESS = "success";

	public static String[] RESULT_HEADER = new String[]{ENTITY_ID, SUCCESS, ORIGINAL_TYPE, NEW_TYPE,CREATED_BY, ERROR};
	
	SynapseClient synapse;
	/**
	 * 
	 * @param synapse
	 */
	MigrateLocationables(SynapseClient synapse){
		this.synapse = synapse;
	}
	
	/**
	 * Migrate all locationables.
	 * @throws Exception
	 */
	private void migrateAll() throws Exception{
		LinkedHashSet<String> locationables = getAllLocationableIds();
		// Migrate each and save the results to a temp file.
		File temp = File.createTempFile("LocationMigration", ".csv");
		System.out.println("Writting results to: "+temp.getAbsolutePath());
		// break it into batches.
		Iterable<List<String>> batches = Iterables.partition(locationables, BATCH_SIZE);
		OutputStream fos = new FileOutputStream(temp);
		try{
			CSVWriter writer = new CSVWriter(new OutputStreamWriter(fos, "UTF-8"));
			writer.writeNext(RESULT_HEADER);
			// Start and wait for each batch
			BasicProgress progress = new BasicProgress();
			progress.setCurrent(0);
			progress.setTotal(locationables.size());
			for(List<String> batch: batches){
				startAndWaitForJob(batch, writer, progress);
			}
			writer.flush();
			progress.setDone();
			System.out.println(progress.getCurrentStatus().toString());
		}finally{
			fos.close();
		}
		System.out.println("Results file: "+temp.getAbsolutePath());
	}
	/**
	 * Start a job and wait for it to finish.
	 * @param batch
	 * @param writer
	 * @param progress
	 * @throws SynapseException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void startAndWaitForJob(List<String> batch, CSVWriter writer, BasicProgress progress) throws SynapseException, IOException, InterruptedException{
		AsyncLocationableTypeConversionRequest request = new AsyncLocationableTypeConversionRequest();
		request.setLocationableIdsToConvert(batch);
		long startProgress = progress.getCurrent();
		String jobId = synapse.startLocationableTypeConvertJob(request);
		// Wait for the job
		while(true){
			try {
				AsyncLocationableTypeConversionResults results = synapse.getLocationableTypeConverJobResults(jobId);
				writeResults(results.getResults(), writer);
				break;
			} catch(SynapseResultNotReadyException e){
				AsynchronousJobStatus status = e.getJobStatus();
				if(AsynchJobState.FAILED.equals(status.getJobState())){
					System.out.println("Job failed: "+jobId+" message: "+status.getErrorMessage());
					System.out.println("Job failed: "+jobId+" details: "+status.getErrorDetails());
					break;
				}
				if(status.getProgressCurrent() != null){
					progress.setCurrent(startProgress+status.getProgressCurrent());
				}
			} catch (SynapseException e) {
				// Something else went wrong.
				e.printStackTrace();
				break;
			}
			System.out.println(progress.getCurrentStatus().toString());
			Thread.sleep(1000);
		}
		// Update the progress when done.
		progress.setCurrent(startProgress+batch.size());
	}

	/**
	 * Wait for each job and record successes.
	 * @param jobIds
	 * @param writer
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	private void waitForAllJobs(Iterator<Pair<String, BasicProgress>> jobIds, CSVWriter writer) throws InterruptedException, IOException {
		while(jobIds.hasNext()){
			// Is this job done
			Pair<String, BasicProgress> pair = jobIds.next();
			String jobId = pair.getFirst();
			BasicProgress progress = pair.getSecond();
			try{
				AsyncLocationableTypeConversionResults results = synapse.getLocationableTypeConverJobResults(jobId);
				writeResults(results.getResults(), writer);
				// Done with this job.
				progress.setDone();
				jobIds.remove();
			}catch(SynapseResultNotReadyException e){
				AsynchronousJobStatus status = e.getJobStatus();
				if(AsynchJobState.FAILED.equals(status.getJobState())){
					System.out.println("Job failed: "+jobId+" message: "+status.getErrorMessage());
					System.out.println("Job failed: "+jobId+" details: "+status.getErrorDetails());
					jobIds.remove();
				}else{
					if(status.getProgressCurrent() != null){
						progress.setCurrent(status.getProgressCurrent());
					}
					if(status.getProgressTotal() != null){
						progress.setTotal(status.getProgressTotal());
					}
					progress.setMessage(status.getProgressMessage());
				}
			} catch (SynapseException e) {
				// Something else went wrong.
				e.printStackTrace();
			}
			// Sleep between each check
			Thread.sleep(100);
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
			set(array, ENTITY_ID, result.getEntityId());
			set(array, SUCCESS, result.getSuccess().toString());
			set(array, ORIGINAL_TYPE, result.getOriginalType());
			set(array, NEW_TYPE, result.getNewType());
			set(array, CREATED_BY, result.getCreatedBy());
			set(array, ERROR, result.getErrorMessage());
			writer.flush();
		}
	}


	/**
	 * Start all of the jobs.
	 * @param locationables
	 * @return
	 * @throws SynapseException
	 */
	private List<Pair<String, BasicProgress>> startAllJobs(LinkedHashSet<String> locationables, AggregateProgress aggProgress)	throws SynapseException {
		List<Pair<String, BasicProgress>> jobIds = Lists.newLinkedList();
		Iterable<List<String>>batches = Iterables.partition(locationables, BATCH_SIZE);
		for(List<String> batch: batches){
			// Submit as a batch
			AsyncLocationableTypeConversionRequest request = new AsyncLocationableTypeConversionRequest();
			request.setLocationableIdsToConvert(batch);
			String jobId = synapse.startLocationableTypeConvertJob(request);
			BasicProgress progress = new BasicProgress();
			aggProgress.addProgresss(progress);
			jobIds.add(Pair.create(jobId, progress));
			System.out.println("Started job: "+jobId);
		}
		return jobIds;
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
	
	/**
	 * Get all locationable ids using pagination.
	 * @return
	 * @throws JSONException
	 * @throws SynapseException
	 * @throws InterruptedException 
	 */
	private LinkedHashSet<String> getAllLocationableIds() throws JSONException, SynapseException, InterruptedException {
		long totalNumber = 0;
		long limit = BATCH_SIZE;
		long offset = 1;
		LinkedHashSet<String> set = new LinkedHashSet<String>();
		System.out.println("Gathering locationable ids...");
		BasicProgress progress = new BasicProgress();
		progress.setMessage("Gathering locationable IDs");
		// First find all locationables
		do{
			String query = "select id from locationable limit "+limit+" offset "+offset;
			JSONObject results = queryWithRetry(query);
			totalNumber = results.getLong("totalNumberOfResults");
			JSONArray array = results.getJSONArray("results");
			for(int i=0; i<array.length(); i++){
				JSONObject ob = (JSONObject) array.get(i);
				String entityId = ob.getString("locationable.id");
				set.add(entityId);
			}
			// next page
			offset = offset+limit;
			progress.setCurrent(set.size());
			progress.setTotal(totalNumber);
			System.out.println(progress.getCurrentStatus().toStringHours());
			if(set.size() > MAX_ENTITES_PER_RUN){
				break;
			}
		}while(set.size() < totalNumber);
		System.out.println("Found: "+set.size()+" Locationables");
		progress.setDone();
		progress.setMessage("Found: "+set.size()+" Locationables");
		System.out.println(progress.getCurrentStatus().toStringHours());
		return set;
	}

	public JSONObject queryWithRetry(String query) throws SynapseException, InterruptedException {
		try {
			return synapse.query(query);
		} catch (Exception e) {
			System.out.println("Will retry:"+e.getMessage());
			Thread.sleep(10000);
			try {
				return synapse.query(query);
			} catch (Exception e2) {
				System.out.println("Will retry:"+e.getMessage());
				Thread.sleep(10000);
				return synapse.query(query);
			}
		}
	}

	/**
	 * main application.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		SynapseClient synapse = SynapseClientUtils.createSynapseConnection(args);
		new MigrateLocationables(synapse).migrateAll();
	}

}
