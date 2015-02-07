package org.sagebionetworks.migrate;

import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;

public class MigrateLocationables {
	
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
		List<String> locationables = getAllLocationableIds();
	}
	
	private List<String> getAllLocationableIds() throws JSONException, SynapseException {
		// First find all locationables
		JSONObject results = synapse.query("select id from locationable");
		JSONArray array = results.getJSONArray("results");
		List<String> list = new LinkedList<String>();
		for(int i=0; i<array.length(); i++){
			JSONObject ob = (JSONObject) array.get(i);
			String entityId = ob.getString("locationable.id");
			System.out.println(entityId);
		}
		return list;
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
