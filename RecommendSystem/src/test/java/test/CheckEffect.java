package test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.blake.database.DBConnection;
import com.blake.share.Tables;

public class CheckEffect {

	private int count = 0;
	
	@Test
	public void test() {
		
		DBConnection dbcon = new DBConnection();
		Connection conWorkspace = dbcon.makeWorkspaceConnection();
		
		String sql = "select real_rating, mib_rating, fullcf_rating, cross_rating from "
				+ Tables.fromString("recommendation").getTableName()
				+ " where mib_overlap=1";
		
		PreparedStatement pre;
		int incre = 0;
		try {
			
			pre = conWorkspace.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
			
				double real_rating = rs.getDouble(1) / 5;
				double mib_rating = rs.getDouble(2) / 5;
				double fullcf_rating = rs.getDouble(3) / 5;
				if(!isGoodRecommend(real_rating, mib_rating, fullcf_rating)) {
					
					count++;
				}
				incre ++;
			}
		} catch (SQLException e) {
			
			
			e.printStackTrace();
		}
		System.out.print("totalcount : "+ incre + " usefulconut : " + count + " ratio is : "+ (double)count/(double)incre);
	}

	private boolean isGoodRecommend(double real_rating, double mib_rating,
			double fullcf_rating) {

		double RMSE_mib = Math.pow(mib_rating - real_rating, 2);
		double RMSE_fullcf = Math.pow(fullcf_rating - real_rating, 2);
		if(RMSE_mib > RMSE_fullcf) {
			return false;
		}
		return true;
	}

}
