package com.blake.data.organize;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

public class ReArrangeCategory {
	
	Connection conWorkspace;
	public HashMap<Integer,Integer> categoryorder = new HashMap<Integer,Integer>();
	public HashMap<Integer,String> categoryStr = new HashMap<Integer,String>();
	
	public ReArrangeCategory(Connection conWorkspace) {
		
		this.conWorkspace = conWorkspace;
	}

	public void reArrangeCategory() {
		
		prepareTable();
		
		PreparedStatement pre ;
		try {
			
			pre = conWorkspace.prepareCall("SELECT * FROM mib.levelcategory order by level");
			ResultSet rs = pre.executeQuery();
			int num = 1;
			while(rs.next()){
				
				int cat = rs.getInt(2);
				String str = rs.getString(3);
				categoryorder.put(cat, num);
				categoryStr.put(num, str);
				num++;
			}
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
		
		
		changeTable();
	}
	
	private void changeTable() {

		PreparedStatement pre ;
		try {
			
			Iterator<Entry<Integer, Integer>> it = categoryorder.entrySet().iterator();
			while(it.hasNext()){
				
				Entry<Integer,Integer> entry = (Entry<Integer, Integer>) it.next();
				pre = conWorkspace.prepareCall("UPDATE levelcategory SET categoryincre = ? , categorynewstr = ? where category = ?;");
				pre.setInt(1, entry.getValue());
				String [] split = categoryStr.get(entry.getValue()).trim().split(" ");
				StringBuilder sb = new StringBuilder();
				for(String s:split) {
					
					if(categoryorder.containsKey(Integer.valueOf(s))) {
						
						sb.append(categoryorder.get(Integer.valueOf(s)) + " ");
					}
				}
				pre.setString(2, sb.toString());
				pre.setInt(3, entry.getKey());
				
				pre.executeUpdate();
				pre.close();
			}

		} catch (SQLException e) {

			e.printStackTrace();
		}
	}

	private void prepareTable() {
		
		PreparedStatement pre ;
		try {
			
			pre = conWorkspace.prepareCall("alter table levelcategory add column categoryincre int(8)");
			pre.executeUpdate();
			pre.close();
			
		} catch (SQLException e) {

//			e.printStackTrace();
		}
		
		try {
			
			pre = conWorkspace.prepareCall("alter table levelcategory add column categorynewstr varchar(30);");
			pre.executeUpdate();
			pre.close();
			
		} catch (SQLException e) {

//			e.printStackTrace();
		}
		
	}
}
