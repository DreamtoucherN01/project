package com.blake.data.organize;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import com.blake.share.HashMapHarness;

public class ImportanceGenerator {
	
	Connection conWorkspace;
	HashMapHarness hm;

	public ImportanceGenerator(Connection conWorkspace, HashMapHarness hm) {

		this.conWorkspace = conWorkspace;
		this.hm = hm;
	}
	
	public double getImportance(int groupId, String givenUserId, String[] userIds) {
		
		PreparedStatement pre;
		int curUserReview = -1;
		int totalReview = 0;
		int ratingNumber = 0;
		try{ 
			
			pre = conWorkspace.prepareStatement("SELECT userTotalReview FROM mib.group_user "
					+ "where group_id = ? and user_id = ?;");
			pre.setInt(1, groupId);
			pre.setString(2, givenUserId);
			ResultSet rs = pre.executeQuery();
			if(rs.next()) {
				
				curUserReview = rs.getInt(1);
				
			} else {
				
				return 0;
			}
			
			pre = conWorkspace.prepareStatement("SELECT userTotalReview FROM mib.group_user "
					+ "where group_id = ?;");
			pre.setInt(1, groupId);
			ResultSet rs1 = pre.executeQuery();
			while(rs1.next()) {
				
				totalReview += rs1.getInt(1);
			} 
			
			HashMap<Integer, Double> pidRatingHM = hm.uidPidRatingHMInTrain.get(Integer.valueOf(givenUserId));

			for(String user : userIds) {
				
				ratingNumber += hm.uidPidRatingHMInTrain.get(Integer.valueOf(user)).size();
			}
			
			return curUserReview / totalReview + pidRatingHM.size() / ratingNumber;
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return 0;
	}
	
	public void checkContain() {
		
	}
}
