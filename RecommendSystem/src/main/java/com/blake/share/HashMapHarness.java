package com.blake.share;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

public class HashMapHarness {
	
	Connection con;
	String[] strTemp = {};
	
	public HashMapHarness(Connection con) {
		
		this.con = con;
	}

	public HashMap<Integer,String> categoryItems = new HashMap<Integer,String>();
	public HashMap<Integer, HashMap<Integer, Double>> uidPidRatingHMInTest = new HashMap<Integer, HashMap<Integer, Double>>();
	public HashMap<Integer, HashMap<Integer, Double>> pidUidRatingHMInTest = new HashMap<Integer, HashMap<Integer, Double>>();
	
	public HashMap<Integer, HashMap<Integer, Double>> uidPidRatingHMInTrain = new HashMap<Integer, HashMap<Integer, Double>>();
	public HashMap<Integer, HashMap<Integer, Double>> pidUidRatingHMInTrain = new HashMap<Integer, HashMap<Integer, Double>>();
	
	public HashMap<Integer, Integer[]> useridsInPid = new HashMap<Integer, Integer[]>();
	public HashMap<Integer, String> itemCategoryStr = new HashMap<Integer, String>();
	public HashMap<Integer, Integer> userImportance = new HashMap<Integer, Integer>();
	public String userIdInTrain[];
	
	public void getHashMap() {
		
		System.out.println(this.getClass().getName() + " getHashMap");
		long begin = System.currentTimeMillis();
		
		getUserCategoryStr();
		getCategoryItemsHM();
		getHMInTest();
		getHMInTrain();
		
		System.out.println("getHashMap done , time cost is " + (System.currentTimeMillis() - begin)/1000);
	}
	
	public void releaseHashMap() {
		
		categoryItems = null;
		uidPidRatingHMInTest = null;
		pidUidRatingHMInTest = null;
		useridsInPid = null;
		itemCategoryStr = null;
		userImportance = null;
		userIdInTrain = null;
	}

	public void getUserCategoryStr() {
		
		PreparedStatement pre;
		try {
			
			pre = con.prepareCall("select item,levelcategory.categorystr from "
					+ "itemcategory,levelcategory where levelcategory.category = itemcategory.category");
			ResultSet rs = pre.executeQuery();
			while(rs.next()){
				
				
				int item = rs.getInt(1);
				String categorystr =  rs.getString(2);
				itemCategoryStr.put(item, categorystr);
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		} finally {
			
			pre = null;
		}
	}
	
	public void getCategoryItemsHM() {
		
		String sql = "SELECT * FROM " + Tables.itemcategory.getTableName() ;
		PreparedStatement pre;
		try {
			pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				int item = rs.getInt(1);
				int category = rs.getInt(2);
					
				String itemIdStr = categoryItems.get(category);
				if(null != itemIdStr) {
					
					itemIdStr += " " + item;
					categoryItems.put(category, itemIdStr);
				} else {
					
					categoryItems.put(category, String.valueOf(item));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void getHMInTest() {

//		Integer[] templateStr = {};
		HashMap<Integer, HashSet<Integer>> useridsInPidTemp = new HashMap<Integer, HashSet<Integer>>();
//		HashSet<String> userIdInTestTmp = new HashSet<String>();
		try {
			String sql = "select user,item,rating,importance from " + Tables.useritemratingtest.getTableName();
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				int uid = rs.getInt(1);
				int pid = rs.getInt(2);
				Double rating = new Double(rs.getDouble(3));
//				int importance = rs.getInt(4);
				
				
				HashMap<Integer, Double> uidRatingHM = pidUidRatingHMInTest.get(pid);
				HashMap<Integer, Double> pidRatingHM = uidPidRatingHMInTest.get(uid);
				HashSet<Integer> uidHS = useridsInPidTemp.get(pid);
				if (uidRatingHM == null) {
					uidRatingHM = new HashMap<Integer, Double>();
				}
				if( null == pidRatingHM ) {
					pidRatingHM = new HashMap<Integer, Double>(); 
				}
				if(null == uidHS) {
					uidHS = new HashSet<Integer>();
				}
				uidRatingHM.put(uid, rating);
				pidRatingHM.put(pid, rating);
				uidHS.add(uid);
				pidUidRatingHMInTest.put(pid, uidRatingHM);
				uidPidRatingHMInTest.put(uid, pidRatingHM);
				
				useridsInPidTemp.put(pid, uidHS);
//				useridsInPid.put(pid, uidHS.toArray(templateStr));
//				userImportance.put(uid, importance);
//				userIdInTestTmp.add(String.valueOf(rs.getInt(1)));
			}
			
			useridsInPidTemp = null;
//			userIdInTest = userIdInTestTmp.toArray(strTemp);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	public void getHMInTrain() {

		Integer[] templateStr = {};
		HashMap<Integer, HashSet<Integer>> useridsInPidTemp = new HashMap<Integer, HashSet<Integer>>();
		HashSet<String> userIdInTestTmp = new HashSet<String>();
		try {
			String sql = "select user,item,rating,importance from " + Tables.useritemrating.getTableName();
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				int uid = rs.getInt(1);
				int pid = rs.getInt(2);
				Double rating = new Double(rs.getDouble(3));
				int importance = rs.getInt(4);
				
				
				HashMap<Integer, Double> uidRatingHM = pidUidRatingHMInTrain.get(pid);
				HashMap<Integer, Double> pidRatingHM = uidPidRatingHMInTrain.get(uid);
				HashSet<Integer> uidHS = useridsInPidTemp.get(pid);
				if (uidRatingHM == null) {
					uidRatingHM = new HashMap<Integer, Double>();
				}
				if( null == pidRatingHM ) {
					pidRatingHM = new HashMap<Integer, Double>(); 
				}
				if(null == uidHS) {
					uidHS = new HashSet<Integer>();
				}
				uidRatingHM.put(uid, rating);
				pidRatingHM.put(pid, rating);
				uidHS.add(uid);
				pidUidRatingHMInTrain.put(pid, uidRatingHM);
				uidPidRatingHMInTrain.put(uid, pidRatingHM);
				
				useridsInPidTemp.put(pid, uidHS);
				useridsInPid.put(pid, uidHS.toArray(templateStr));
				userImportance.put(uid, importance);
				userIdInTestTmp.add(String.valueOf(rs.getInt(1)));
			}
			
			useridsInPidTemp = null;
			userIdInTrain = userIdInTestTmp.toArray(strTemp);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
}
