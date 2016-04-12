package com.blake.database.generator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.blake.share.Tables;

public abstract class DatabaseOperation {

	Connection con;
	
	public DatabaseOperation(Connection con) {
		
		this.con = con;
	}
	
	public void createTables() {
		
		PreparedStatement pre;
		
		try{
			
			String sql = "CREATE TABLE IF NOT EXISTS " 
					+ Tables.fromString("useritemrating").getTableName() 
					+ "("
	                + "  user int(16) NOT NULL,"
	                + "  item int(16) NOT NULL,"
	                + "  rating double,"
	                + "  importance int(4),"
	                + "  PRIMARY KEY  (user,item)"
	                + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		try{
			
			String sql = "CREATE TABLE IF NOT EXISTS " 
					+ Tables.fromString("categoryparent").getTableName() 
					+ "("
	                + "  category int(16) NOT NULL,"
	                + "  parent int(16)"
	                + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		try{
			
			String sql = "CREATE TABLE IF NOT EXISTS " 
					+ Tables.fromString("itemcategory").getTableName() 
					+ "("
	                + "  item int(11),"
	                + "  category int(11)"
	                + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		try{
			
			String sql = "CREATE TABLE IF NOT EXISTS " 
					+ Tables.fromString("levelcategory").getTableName() 
					+ "("
	                + "  level int(4),"
	                + "  category int(11),"
	                + "  categorystr varchar(128)"
	                + ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		try {
			
			String sql = "CREATE TABLE IF NOT EXISTS "
					+ Tables.fromString("grouptable").getTableName()
					+ " ("
					+ "  id int(16) NOT NULL AUTO_INCREMENT,"
					+ "  level int(16) NOT NULL,"
					+ "  itemsId varchar(1024) collate utf8_unicode_ci NOT NULL,"
					+ "  support int(16) collate utf8_unicode_ci NOT NULL,"
					+ "  usersId varchar(1024) collate utf8_unicode_ci default NULL,"
					+ "  totalReview int(16) collate utf8_unicode_ci default NULL,"
					+ "  totalItemNumber int(16) collate utf8_unicode_ci default NULL,"
					+ "  KEY id (id)"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}

		try {
			
			String sql = "CREATE TABLE IF NOT EXISTS "
					+ Tables.fromString("group_category").getTableName()
					+ " ("
					+ "  group_id int(16) NOT NULL,"
					+ "  item_id int(16) NOT NULL,"
					+ "  sim double collate utf8_unicode_ci NOT NULL"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}

		try {
			String sql = "CREATE TABLE IF NOT EXISTS "
					+ Tables.fromString("group_user").getTableName()
					+ " ("
					+ "  group_id int(16) NOT NULL,"
					+ "  user_id int(16) NOT NULL,"
					+ "  userTotalReview int(16) collate utf8_unicode_ci NOT NULL,"
					+ "  importance double collate utf8_unicode_ci NOT NULL"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		try {
			String sql = "CREATE TABLE IF NOT EXISTS "
					+ Tables.fromString("recommendation").getTableName()
					+ " ("
					+ "  uid varchar(128) NOT NULL,"
					+ "  pid varchar(128) collate utf8_unicode_ci NOT NULL,"
					+ "  real_rating double collate utf8_unicode_ci NOT NULL,"
					+ "  mib_rating double collate utf8_unicode_ci default NULL,"
					+ "  fullcf_rating double NOT NULL,"
					+ "  cross_rating double collate utf8_unicode_ci NOT NULL,"
					+ "  mib_overlap int(16) collate utf8_unicode_ci NOT NULL,"
					+ "  fullcf_overlap int(16) collate utf8_unicode_ci default NULL,"
					+ "  cross_overlap int(16) collate utf8_unicode_ci default NULL"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
			pre = con.prepareCall(sql);
			pre.executeUpdate();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	public void truncateTables() {
		
		PreparedStatement pre;
		for(Tables table : Tables.values()) {
			
			if(filterTable(table.getTableName())) {
				
				continue;
			}
			
			try {
				
				pre = con.prepareCall("truncate table " + table.getTableName());
				pre.execute();
				pre.close();
				pre = null;
			} catch (SQLException e) {

				e.printStackTrace();
			}
		}
	}
	
	private boolean filterTable(String tableName) {

		if(("review").equals(tableName) || ("product").equals(tableName)
				|| ("category").equals(tableName)) {
			
			return true;
		}
		return false;
	}

	public void deleteTables() {
		
		PreparedStatement pre;
		for(Tables table : Tables.values()) {
			
			try {
				
				pre = con.prepareCall("drop table " + table.getTableName());
				pre.execute();
				pre.close();
				pre = null;
			} catch (SQLException e) {

				e.printStackTrace();
			}
		}
	}
}
