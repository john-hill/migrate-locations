package org.sagebionetworks.migrate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.PaginatedResults;
import org.sagebionetworks.repo.model.UserProfile;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;

import au.com.bytecode.opencsv.CSVWriter;

public class ProfileData {

	public static void main(String[] args) throws SynapseException, JSONObjectAdapterException, IOException {
		SynapseClient synapse = SynapseClientUtils.createSynapseConnection(args);
		// Write all profile data to a CSV
		File temp = File.createTempFile("UserNames", ".csv");
		System.out.println("Output file: "+temp.getAbsolutePath());
		CSVWriter writer = new CSVWriter(new FileWriter(temp));
		try{
			writer.writeNext(new String[]{"name","principalId"});
			String[] buffer = new String[2];
			int offset = 0;
			while(true){

				PaginatedResults<UserProfile> list = synapse.getUsers(offset, 100);
				if(list.getResults() == null || list.getResults().size() < 1){
					break;
				}
				for(UserProfile profile: list.getResults()){
					buffer[1] = profile.getOwnerId().trim();
					String first = profile.getFirstName();
					if(first == null){
						first = "";
					}
					first = first.trim().toLowerCase();
					String last = profile.getLastName();
					if(last == null){
						last = "";
					}
					last = last.trim().toLowerCase();
					// first last
					buffer[0] = first+last;
					if(!"".equals(buffer[0])){
						writer.writeNext(buffer);
					}
					// last first
					buffer[0] = last+first;
					if(!"".equals(buffer[0])){
						writer.writeNext(buffer);
					}
					String userName = profile.getUserName();
					if(userName == null){
						userName = "";
					}
					userName = userName.trim().toLowerCase();
					buffer[0] = userName;
					if(!"".equals(buffer[0])){
						writer.writeNext(buffer);
					}
				}
				offset += list.getResults().size();
			}
			
		}finally{
			writer.close();
		}
		System.out.println("Output file: "+temp.getAbsolutePath());
	}

}
