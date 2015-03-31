package org.sagebionetworks.migrate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.AsyncLocationableTypeConversionRequest;
import org.sagebionetworks.tool.progress.BasicProgress;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.common.collect.Iterables;

/**
 * Start all migration jobs using the provided query.
 * 
 * @author jhill
 *
 */
public class StartMigrationJobs {
	
	private static final long MAX_ENTITES_PER_RUN = Long.MAX_VALUE;
	private static final int BATCH_SIZE = 1000;
	SynapseClient synapse;
	
	public StartMigrationJobs(SynapseClient client, String queryString) throws Exception {
		this.synapse = client;
		LinkedHashSet<String> locationables = getAllLocationableIds(queryString);
		
		File temp = File.createTempFile("LocationMigrationJobs", ".csv");
		System.out.println("Writting results to: "+temp.getAbsolutePath());
		// break each job into a batch.
		Iterable<List<String>> batches = Iterables.partition(locationables, BATCH_SIZE);
		BasicProgress progress = new BasicProgress();
		progress.setMessage("Starting jobs");
		progress.setCurrent(0);
		progress.setTotal(locationables.size());
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(temp), "UTF-8"));
		String[] array = new String[1];
		try{
			for(List<String> ids: batches){
				AsyncLocationableTypeConversionRequest request = new AsyncLocationableTypeConversionRequest();
				request.setLocationableIdsToConvert(ids);
				String jobId = synapse.startLocationableTypeConvertJob(request);
				array[0] = jobId;
				writer.writeNext(array);
				writer.flush();
				progress.setCurrent(progress.getCurrent()+ids.size());
				System.out.println(progress.getCurrentStatus().toStringHours());
			}
		}finally{
			writer.flush();
			writer.close();
		}
		System.out.println("JobIds file: "+temp.getAbsolutePath());
	}

	
	public static void main(String[] args) throws Exception{
		SynapseClient synapse = SynapseClientUtils.createSynapseConnection(args);
		if(args == null || args.length < 6){
			throw new IllegalArgumentException("args[5]=queryString");
		}
		new StartMigrationJobs(synapse, args[5]);
	}
	
	/**
	 * Get all locationable ids using pagination.
	 * @return
	 * @throws JSONException
	 * @throws SynapseException
	 * @throws InterruptedException 
	 */
	private LinkedHashSet<String> getAllLocationableIds(String queryString) throws JSONException, SynapseException, InterruptedException {
		long totalNumber = 0;
		long limit = BATCH_SIZE;
		long offset = 1;
		LinkedHashSet<String> set = new LinkedHashSet<String>();
		System.out.println("Gathering locationable ids...");
		BasicProgress progress = new BasicProgress();
		progress.setMessage("Gathering locationable IDs");
		// First find all locationables
		do{
			String query = queryString+" limit "+limit+" offset "+offset;
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
}
