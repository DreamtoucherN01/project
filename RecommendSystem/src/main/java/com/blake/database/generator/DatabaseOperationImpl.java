package com.blake.database.generator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;

import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.MyArray;

public class DatabaseOperationImpl  extends DatabaseOperation {

	public DatabaseOperationImpl(Connection con) {
		
		super(con);
	}
	
	public void insertIntoRecommendation(String givenItemId, String givenUid,
			double real_rating, double fullcf_rating, int cf_overlap,
			double last_cross_rating, int cross_overlap,
			double mul_cross_rating, int mib_overlap) {
		
		try {
			String sql = "insert into "
					+ Tables.fromString("recommendation").getTableName()
					+ " (uid, pid, "
					+ "real_rating, mib_rating, fullcf_rating, cross_rating, "
					+ "mib_overlap, fullcf_overlap, cross_overlap) "
					+ "values (?,?,?,?,?,?,?,?,?)";
			PreparedStatement pre = con.prepareCall(sql);
			pre.setString(1, givenUid);
			pre.setString(2, givenItemId);
			pre.setDouble(3, real_rating);
			pre.setDouble(4, mul_cross_rating);
			pre.setDouble(5, fullcf_rating);
			pre.setDouble(6, last_cross_rating);
			pre.setInt(7, mib_overlap);
			pre.setInt(8, cf_overlap);
			pre.setInt(9, cross_overlap);
			pre.executeUpdate();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertIntoGroupTable(int level, String itemsId, int support,
			String usersId, int totalReview, int totalItemNumber) {
		
		try {
			String sql = "insert into " + Tables.fromString("grouptable").getTableName()
					     + " (level,itemsId,support,usersId, totalReview,totalItemNumber) values(?,?,?,?,?,?)";
			PreparedStatement pre = con.prepareCall(sql);
			pre.setInt(1, level);
			pre.setString(2, itemsId);
			pre.setInt(3, support);
			pre.setString(4, usersId);
			pre.setInt(5, totalReview);
			pre.setInt(6, totalItemNumber);
			pre.executeUpdate();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertIntoGroupCategoryTable(int groupId, String[] itemsArr, double[] simDoubleArr) {
		String sql = "insert into " + Tables.fromString("group_category").getTableName()
				+ " (group_id,item_id,sim) values (?,?,?)";
		try {
			
			PreparedStatement pre = con.prepareCall(sql);
			for (int i = 0; i < itemsArr.length; i++) {
				try {
					pre.setInt(1, groupId);
					pre.setInt(2, Integer.parseInt(itemsArr[i]));
					pre.setDouble(3, simDoubleArr[i]);
					pre.executeUpdate();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			pre.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	public void insertIntoGroupUserTable(int groupId, String[] userIdArrSrc,
			String[] itemsIdArr, HashMapHarness hm) {
		
		String[] userIdArr = MyArray.intersect(userIdArrSrc, hm.userIdInTrain);
		String sql = "insert into " + Tables.fromString("group_user").getTableName()
						+ " (group_id,user_id,userTotalReview,importance) values (?,?,?,?)";
		HashMap<Integer, Integer> userTotalReviewHM = new DatabaseOperationHelper(hm.uidPidRatingHMInTrain)
																.getUserTotalReviewByUserIdItemId(userIdArr, itemsIdArr);
		if(null == userTotalReviewHM || userTotalReviewHM.isEmpty()) {
			
			return;
		}
		try {
			
			PreparedStatement pre = con.prepareCall(sql);
			for (int i = 0; i < userIdArr.length; i++) {
				
				if(userTotalReviewHM.containsKey(new Integer(userIdArr[i]))) {
					try {
						pre.setInt(1, groupId);
						pre.setInt(2, Integer.parseInt(userIdArr[i]));
						pre.setInt(3, userTotalReviewHM.get(new Integer(userIdArr[i])));
						pre.setDouble(4, hm.userImportance.get(Integer.parseInt(userIdArr[i])));
						pre.executeUpdate();
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}
			pre.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

}
