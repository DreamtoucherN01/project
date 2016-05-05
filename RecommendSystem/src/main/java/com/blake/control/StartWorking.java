package com.blake.control;

import java.sql.Connection;

import com.blake.category.Group;
import com.blake.data.process.DataTransfer;
import com.blake.database.DBConnection;
import com.blake.database.generator.DatabaseOperation;
import com.blake.database.generator.DatabaseOperationImpl;
import com.blake.effect.Effect;
import com.blake.recommendation.ItemBaseRecommedation;
import com.blake.recommendation.Recommendation;
import com.blake.recommendation.UserBaseRecommendation;
import com.blake.share.HashMapHarness;
import com.blake.util.Constants;

public class StartWorking {
	
	public static void main(String args[]) {
		
		DBConnection dbcon = new DBConnection();
		Connection conSrc = dbcon.makeSourceConnection();         // data source
		Connection conWorkspace = dbcon.makeWorkspaceConnection();// data processor place
		
		DatabaseOperation dbo = new DatabaseOperationImpl(conWorkspace);
//		dbo.createTables();
//		dbo.truncateTables();
//		
//		DataTransfer df = new DataTransfer(conSrc, conWorkspace); // transfer data
//		df.dataTransfer(); dbcon.closeConnection(conSrc); System.gc();
		
		HashMapHarness hm = new HashMapHarness(conWorkspace);    // trade space for mining time
		hm.getHashMap();
		
//		Group ca = new Group(hm, dbo);
//		ca.mineData();                                           // mine data
		 
		if(Constants.IS_USER_BASE) {                             // recommend 
			
			Recommendation re = new UserBaseRecommendation(hm, dbo);
			((UserBaseRecommendation) re).doRecommendationByReviewSplitUserBase();

		} else {
			
			Recommendation re = new ItemBaseRecommedation(hm, dbo);
			((ItemBaseRecommedation) re).doRecommendationByReviewSplitItemBase();
		
		}
		
		Effect effect = new Effect(conWorkspace);                // effect show
		effect.showMAEAndRMSEEffect();
		effect.showOverlapEffect();        
		effect.showMAPAndNDCGEffect(15);
		effect.showMAPAndNDCGEffect(10);
		effect.showMAPAndNDCGEffect(5);
		effect.showMAPAndNDCGEffect(3);
		effect.showMAPAndNDCGEffect(2);
		effect.showMAPAndNDCGEffect(1);
		
		hm.releaseHashMap();
		dbcon.closeConnection();
	}

}
