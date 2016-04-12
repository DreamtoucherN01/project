package com.blake.database.generator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import com.blake.share.Tables;

public class DatabaseOperationHelper{

	Connection con;
	
	public DatabaseOperationHelper(Connection con) {
		
		this.con = con;
	}

	public HashMap<Integer, Integer> getUserTotalReviewByUserIdItemId(
			String[] usersIdArr, String[] itemsIdArr) {
		
		HashMap<Integer, Integer> userTotalReview = new HashMap<Integer, Integer>();
		if (itemsIdArr.length == 0 || usersIdArr.length == 0) {
			
			return userTotalReview;
		}
		try {
			
			String sql = "select user,count(*) from " + Tables.useritemrating.getTableName() +
					"  where user in (" + usersIdArr[0];
			for (int i = 1; i < usersIdArr.length; i++) {
				sql += "," + usersIdArr[i];
			}
			sql += ") and item in (" + itemsIdArr[0];
			for (int i = 1; i < itemsIdArr.length; i++) {
				sql += "," + itemsIdArr[i];
			}
			sql += ") group by user";
			
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				int uid = rs.getInt(1);
				int totalReview = rs.getInt(2);
				userTotalReview.put(new Integer(uid), new Integer(totalReview));
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return userTotalReview;
	}
}
