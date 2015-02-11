package org.sagebionetworks.migrate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.Entity;

import au.com.bytecode.opencsv.CSVWriter;

public class MigrateLocationables {
	
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
		OutputStream fos = new FileOutputStream(temp);
		try{
			CSVWriter writer = new CSVWriter(new OutputStreamWriter(fos, "UTF-8"));
			writer.writeNext(RESULT_HEADER);
			// Write each
			int count = 1;
			for(String id: locationables){
				System.out.println("Migrating "+count+"/"+locationables.size());
				translateLocation(id, writer);
				count++;
			}
			writer.flush();
		}finally{
			fos.close();
		}
		System.out.println("Results file: "+temp.getAbsolutePath());
	}
	
	/**
	 * Translate a single entity.
	 * @param id
	 * @param writer
	 * @throws IOException 
	 */
	private void translateLocation(String id, CSVWriter writer) throws IOException{
		String[] results = new String[RESULT_HEADER.length];
		set(results, ENTITY_ID, id);
		try{
			Entity e = synapse.getEntityById(id);
			set(results, ORIGINAL_TYPE, e.getClass().getName());
			set(results, CREATED_BY, e.getCreatedBy());
			// convert
			e = synapse.convertLocationableEntity(e);
			set(results, NEW_TYPE, e.getClass().getName());
			set(results, SUCCESS, Boolean.TRUE.toString());
		}catch(Exception e){
			set(results, SUCCESS, Boolean.FALSE.toString());
			set(results, ERROR, e.getMessage());
		}
		writer.writeNext(results);
		writer.flush();
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
			System.out.println("Found "+set.size()+"/"+totalNumber);
		}while(set.size() < totalNumber);
		System.out.println("Found: "+set.size()+" Locationables");
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
