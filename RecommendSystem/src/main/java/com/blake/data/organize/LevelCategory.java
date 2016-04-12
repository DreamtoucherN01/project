package com.blake.data.organize;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.blake.share.Tables;

public class LevelCategory {
	
	Connection conWorkspace;
	
	public LevelCategory(Connection conWorkspace) {
		
		this.conWorkspace = conWorkspace;
	}
	
	public void getLevelCategory() {
		
		PreparedStatement pre;
		try {
			
			pre = conWorkspace.prepareCall("SELECT * FROM categoryparent");
			ResultSet rs = pre.executeQuery();
			while(rs.next()){
				
				int category = rs.getInt(1);
				insertIntoLevelCategory(category,category + " " + getCategoryStr(category));
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		} finally {
			
			pre = null;
		}
	}

	private void insertIntoLevelCategory(int category, String categoryStr) {

		String sql="insert into " + Tables.levelcategory.getTableName() + 
				" (level,category,categorystr) values(?,?,?)";
        PreparedStatement pre;
		 try {
			 pre = conWorkspace.prepareCall(sql);
	         pre.setInt(1, getLevel(categoryStr));
	         pre.setInt(2, category);
	         pre.setString(3, categoryStr);
	         pre.executeUpdate();
	         pre.close();
		 } catch (SQLException e) {
			 
//			e.printStackTrace();
		 } finally {
			 
			 pre = null;
		 }
	}

	private int getLevel(String parent) {

		int len = parent.trim().split(" ").length;
		switch(len) {
		
		case 1:
			return 1;
		case 2:
			return 2;
		case 3:
			return 3;
		case 4:
			return 4;
		case 5: 
			return 5;
		}
		
		return 0;
	}

	private String getCategoryStr(int category) {

		PreparedStatement pre;
		try {
			
			pre = conWorkspace.prepareCall("SELECT parent FROM categoryparent where  category = ?");
			pre.setInt(1, category);
			ResultSet rs = pre.executeQuery();
			if(rs.next()){
				
				int par = rs.getInt(1);
				if(0 == par) {
					
					return "";
				}
				return par+ " " +getCategoryStr(par);
			} else {
				
				return "";
			}
		} catch (SQLException e) {
			
			e.printStackTrace();
		} finally {
			
			pre = null;
		}
		return "";
	}
}
