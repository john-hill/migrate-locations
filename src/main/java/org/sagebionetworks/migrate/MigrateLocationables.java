package org.sagebionetworks.migrate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.Entity;

import au.com.bytecode.opencsv.CSVWriter;

public class MigrateLocationables {
	
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
			for(String id: locationables){
				translateLocation(id, writer);
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
	 */
	private void translateLocation(String id, CSVWriter writer){
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
	 */
	private LinkedHashSet<String> getAllLocationableIds() throws JSONException, SynapseException {
		long totalNumber = 0;
		long limit = 1000;
		long offset = 1;
		LinkedHashSet<String> set = new LinkedHashSet<String>();
		// First find all locationables
		do{
			String query = "select id from locationable limit "+limit+" offset "+offset;
			JSONObject results = synapse.query(query);
			totalNumber = results.getLong("totalNumberOfResults");
			JSONArray array = results.getJSONArray("results");
			for(int i=0; i<array.length(); i++){
				JSONObject ob = (JSONObject) array.get(i);
				String entityId = ob.getString("locationable.id");
				set.add(entityId);
			}
			// next page
			offset = offset+limit;
		}while(set.size() < totalNumber);
		System.out.println("Found: "+set.size()+" Locationables");
		return set;
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
