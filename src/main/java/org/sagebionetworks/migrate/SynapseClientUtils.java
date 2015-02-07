package org.sagebionetworks.migrate;

import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;

public class SynapseClientUtils {

	/**
	 * Create a synapse connection using input arguments.
	 * @param args
	 * @return
	 * @throws JSONObjectAdapterException 
	 * @throws SynapseException 
	 */
	public static SynapseClient createSynapseConnection(String[] args) throws SynapseException, JSONObjectAdapterException {
		if(args == null || args.length < 5){
			throw new IllegalArgumentException("Expected args[0]=AuthEndpoint, args[1]=RepositoryEndpoint, args[2]=FilesEndpoint, args[3]=username, args[4]=ApiKey");
		}
		String authEndpoint = args[0];
		String repoEndpoint = args[1];
		String fileEndpoint = args[2];
		String username = args[3];
		String apiKey = args[4];
		System.out.println("AuthEndpoint="+authEndpoint);
		System.out.println("RepoEndpoint="+repoEndpoint);
		System.out.println("FileEndpont="+fileEndpoint);
		System.out.println("Username="+username);
		System.out.println("ApiKey="+apiKey.replaceAll("[\\w\\W]", "*"));
		SynapseClient synapse = new SynapseAdminClientImpl();
		synapse.setAuthEndpoint(authEndpoint);
		synapse.setRepositoryEndpoint(repoEndpoint);
		synapse.setFileEndpoint(fileEndpoint);
		synapse.setUserName(username);
		synapse.setApiKey(apiKey);
		System.out.println(synapse.getVersionInfo().toString());
		return synapse;
	}

}
