package com.blake.data.process;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.blake.data.organize.LevelCategory;
import com.blake.share.Importance;
import com.blake.share.Tables;

public class DataTransfer {
	
	Connection conSrc;
	Connection conWorkspace;
	
	public DataTransfer(Connection conSrc , Connection conWorkspace) {
		
		this.conSrc = conSrc;
		this.conWorkspace = conWorkspace;
	}

	public void dataTransfer() {
		
		System.out.println(this.getClass().getName() + " dataTransfer");
		PreparedStatement pre;
		try {
			
			pre = conSrc.prepareCall("SELECT * FROM review order by iduser");
			ResultSet rs = pre.executeQuery();
			while(rs.next()){
				
				int user = rs.getInt(2);
				if(user > 500) {
					
					break;
				}
				int item = rs.getInt(5);
				int rating = rs.getInt(3);
				String importance = rs.getString(4);
				if(Importance.fromString(importance.split(" ")[0]) == null) {
					
					System.out.println(importance);
				}
				insertintoUserItemRating(user,item,rating,Importance.fromString(importance.split(" ")[0]));
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		pre = null;
		
		try {
			
			pre = conSrc.prepareCall("SELECT * FROM category");
			ResultSet rs = pre.executeQuery();
			while(rs.next()){
				
				int category = rs.getInt(1);
				int parent = rs.getInt(3);
				insertintoCategoryParent(category,parent);
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		pre = null;
		
		try {
			
			pre = conSrc.prepareCall("SELECT * FROM product");
			ResultSet rs = pre.executeQuery();
			while(rs.next()){
				
				int item = rs.getInt(1);
				int category = rs.getInt(2);
				insertintoItemCategory(item,category);
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		pre = null;
		
		LevelCategory lc = new LevelCategory(conWorkspace);
		lc.getLevelCategory();
	}

	private void insertintoItemCategory(int item, int category) {

		String sql="insert into " + Tables.itemcategory.getTableName() + 
				" (item,category) values(?,?)";
        PreparedStatement pre;
		 try {
			 pre = conWorkspace.prepareCall(sql);
	         pre.setInt(1, item);
	         pre.setInt(2, category);
	         pre.executeUpdate();
	         pre.close();
		 } catch (SQLException e) {
//			e.printStackTrace();
		 }
	}

	private void insertintoCategoryParent(int category, int parent) {

		String sql="insert into " + Tables.categoryparent.getTableName() + 
				" (category,parent) values(?,?)";
        PreparedStatement pre;
		 try {
			 pre = conWorkspace.prepareCall(sql);
	         pre.setInt(1, category);
	         pre.setInt(2, parent);
	         pre.executeUpdate();
	         pre.close();
		 } catch (SQLException e) {
//			e.printStackTrace();
		 }
	}

	private void insertintoUserItemRating(int user, int item, int rating,
			Importance importance) {

		String sql="insert into " + Tables.useritemrating.getTableName() + 
				" (user,item,rating,importance) values(?,?,?,?)";
        PreparedStatement pre;
		 try {
			 pre = conWorkspace.prepareCall(sql);
	         pre.setInt(1, user);
	         pre.setInt(2, item);
	         pre.setDouble(3, rating);
	         pre.setInt(4, importance.getLevel());
	         pre.executeUpdate();
	         pre.close();
		 } catch (SQLException e) {
//			e.printStackTrace();
		 }
	}
	
}
