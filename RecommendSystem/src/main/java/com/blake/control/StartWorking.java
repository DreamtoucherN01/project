package com.blake.control;

import java.sql.Connection;

import com.blake.category.Category;
import com.blake.data.process.DataTransfer;
import com.blake.database.DBConnection;
import com.blake.database.generator.DatabaseOperation;
import com.blake.database.generator.DatabaseOperationImpl;
import com.blake.effect.Effect;
import com.blake.recommendation.Recommendation;
import com.blake.share.HashMapHarness;

public class StartWorking {
	
	public static void main(String args[]) {
		
		DBConnection dbcon = new DBConnection();
		Connection conSrc = dbcon.makeSourceConnection();
		Connection conWorkspace = dbcon.makeWorkspaceConnection();
		
		//clear data
		DatabaseOperation dbo= new DatabaseOperationImpl(conWorkspace);
		dbo.createTables();
		dbo.truncateTables();
		
		//java 2 mib
		DataTransfer df = new DataTransfer(conSrc, conWorkspace);
		df.dataTransfer();
		
		//get HashMapData 
		HashMapHarness hm = new HashMapHarness(conWorkspace);
		hm.getHashMap();
		
		//frequent pattern tree
		Category ca = new Category(hm,conWorkspace,dbo);
		ca.mineData();
		
		//recommendation
		Recommendation re = new Recommendation(hm,conWorkspace,dbo);
		re.doRecommendationByReviewSplitItemBase();
		
		hm.releaseHashMap();
		
		//show recommendation effect
		Effect effect = new Effect(conWorkspace);
		effect.showMAEAndRMSEEffect();
		effect.showOverlapEffect();        
		effect.showMAPAndNDCGEffect(15);
		effect.showMAPAndNDCGEffect(10);
		effect.showMAPAndNDCGEffect(5);
		effect.showMAPAndNDCGEffect(3);
		effect.showMAPAndNDCGEffect(2);
		effect.showMAPAndNDCGEffect(1);
		
		dbcon.closeConnection();
	}

}
