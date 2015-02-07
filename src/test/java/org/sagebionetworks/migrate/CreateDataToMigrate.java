package org.sagebionetworks.migrate;

import java.util.Random;
import java.util.UUID;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.Data;
import org.sagebionetworks.repo.model.ExpressionData;
import org.sagebionetworks.repo.model.GenotypeData;
import org.sagebionetworks.repo.model.Locationable;
import org.sagebionetworks.repo.model.PhenotypeData;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Study;


public class CreateDataToMigrate {
	
	private static String[] FILE_TYPES = new String[]{
		Data.class.getName(),
		PhenotypeData.class.getName(),
		GenotypeData.class.getName(),
		PhenotypeData.class.getName(),
		ExpressionData.class.getName(),
	};

	public static void main(String[] args) throws Exception {
		SynapseClient synapse = SynapseClientUtils.createSynapseConnection(args);
		// Create project
		Project project = new Project();
		project.setName(UUID.randomUUID().toString());
		project = synapse.createEntity(project);
		System.out.println("Created Project: "+project.getId());
		
		Random rand = new Random(123);
		// Add some studies to the project.
		int folders = rand.nextInt(4)+1;
		int count = 0;
		for(int folder=0; folder<folders; folder++){
			Study study = new Study();
			study.setParentId(project.getId());
			study.setName("Study"+folder);
			study = synapse.createEntity(study);
			count++;
			int files = rand.nextInt(4)+1;
			for(int file=0; file<files; file++){
				int type = rand.nextInt(FILE_TYPES.length-1);
				Locationable l = (Locationable) Class.forName(FILE_TYPES[type]).newInstance();
				l.setName("File"+file);
				l.setParentId(study.getId());
				l = synapse.createEntity(l);
				count++;
			}
		}
		System.out.println("Created: "+count+" locationables");

	}

}
