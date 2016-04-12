/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blake.effect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Iterator;

import com.blake.share.Tables;
import com.blake.util.Constants;

/**
 *
 * @author Leo Lee
 */
public class Effect {
	private Connection con;

	public Effect(Connection con) {
		this.con = con;
	}

	public void showMAEAndRMSEEffect() {
		double MAE_mib = 0;
		double RMSE_mib = 0;
		int N_mib = 0;

		double MAE_fullcf = 0;
		double RMSE_fullcf = 0;
		int N_fullcf = 0;

		double MAE_cross = 0;
		double RMSE_cross = 0;
		int N_cross = 0;

		try {
			String sql = "select real_rating, mib_rating, fullcf_rating, cross_rating from "
					+ Tables.fromString("recommendation").getTableName()
					+ " where fullcf_overlap=1";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				double real_rating = rs.getDouble(1) / 5;
				double mib_rating = rs.getDouble(2) / 5;
				double fullcf_rating = rs.getDouble(3) / 5;
				double cross_rating = rs.getDouble(4) / 5;

				if (mib_rating > 0) {
					MAE_mib += Math.abs(mib_rating - real_rating);
					RMSE_mib += Math.pow(mib_rating - real_rating, 2);
					N_mib++;
				}
				if (fullcf_rating > 0) {
					MAE_fullcf += Math.abs(fullcf_rating - real_rating);
					RMSE_fullcf += Math.pow(fullcf_rating - real_rating, 2);
					N_fullcf++;
				}
				if (cross_rating > 0) {
					MAE_cross += Math.abs(cross_rating - real_rating);
					RMSE_cross += Math.pow(cross_rating - real_rating, 2);
					N_cross++;
				}
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		MAE_mib = MAE_mib / N_mib;
		RMSE_mib = Math.sqrt(RMSE_mib / N_mib);

		MAE_fullcf = MAE_fullcf / N_fullcf;
		RMSE_fullcf = Math.sqrt(RMSE_fullcf / N_fullcf);

		MAE_cross = MAE_cross / N_cross;
		RMSE_cross = Math.sqrt(RMSE_cross / N_cross);

		System.out.println("cf方法覆盖的那些用户：");
		System.out.println("MAE_mib=" + MAE_mib);
		System.out.println("MAE_cross=" + MAE_cross);
		System.out.println("MAE_fullcf=" + MAE_fullcf);
		System.out.println();
		System.out.println("RMSE_mib=" + RMSE_mib);
		System.out.println("RMSE_cross=" + RMSE_cross);
		System.out.println("RMSE_fullcf=" + RMSE_fullcf);
		System.out.println();

		MAE_mib = 0;
		RMSE_mib = 0;
		N_mib = 0;

		MAE_fullcf = 0;
		RMSE_fullcf = 0;
		N_fullcf = 0;

		MAE_cross = 0;
		RMSE_cross = 0;
		N_cross = 0;

		try {
			String sql = "select real_rating, mib_rating, fullcf_rating, cross_rating from "
					+ Tables.fromString("recommendation").getTableName()
					+ " where mib_overlap=1 and cross_overlap=0";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				double real_rating = rs.getDouble(1) / 5;
				double mib_rating = rs.getDouble(2) / 5;
				double fullcf_rating = rs.getDouble(3) / 5;
				double cross_rating = rs.getDouble(4) / 5;

				if (mib_rating > 0) {
					MAE_mib += Math.abs(mib_rating - real_rating);
					RMSE_mib += Math.pow(mib_rating - real_rating, 2);
					N_mib++;
				}
				if (fullcf_rating > 0) {
					MAE_fullcf += Math.abs(fullcf_rating - real_rating);
					RMSE_fullcf += Math.pow(fullcf_rating - real_rating, 2);
					N_fullcf++;
				}
				if (cross_rating > 0) {
					MAE_cross += Math.abs(cross_rating - real_rating);
					RMSE_cross += Math.pow(cross_rating - real_rating, 2);
					N_cross++;
				}
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		MAE_mib = MAE_mib / N_mib;
		RMSE_mib = Math.sqrt(RMSE_mib / N_mib);

		MAE_fullcf = MAE_fullcf / N_fullcf;
		RMSE_fullcf = Math.sqrt(RMSE_fullcf / N_fullcf);

		MAE_cross = MAE_cross / N_cross;
		RMSE_cross = Math.sqrt(RMSE_cross / N_cross);

		System.out.println("mib方法覆盖但cross方法未覆盖的那些用户：");
		System.out.println("MAE_mib=" + MAE_mib);
		System.out.println("MAE_cross=" + MAE_cross);
		System.out.println("MAE_fullcf=" + MAE_fullcf);
		System.out.println();
		System.out.println("RMSE_mib=" + RMSE_mib);
		System.out.println("RMSE_cross=" + RMSE_cross);
		System.out.println("RMSE_fullcf=" + RMSE_fullcf);

		MAE_mib = 0;
		RMSE_mib = 0;
		N_mib = 0;

		MAE_fullcf = 0;
		RMSE_fullcf = 0;
		N_fullcf = 0;

		MAE_cross = 0;
		RMSE_cross = 0;
		N_cross = 0;

		try {
			String sql = "select real_rating, mib_rating, fullcf_rating, cross_rating from "
					+ Tables.fromString("recommendation").getTableName() + " where mib_overlap=1";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				double real_rating = rs.getDouble(1) / 5;
				double mib_rating = rs.getDouble(2) / 5;
				double fullcf_rating = rs.getDouble(3) / 5;
				double cross_rating = rs.getDouble(4) / 5;
				if (mib_rating > 0) {
					MAE_mib += Math.abs(mib_rating - real_rating);
					RMSE_mib += Math.pow(mib_rating - real_rating, 2);
					N_mib++;
				}
				if (fullcf_rating > 0) {
					MAE_fullcf += Math.abs(fullcf_rating - real_rating);
					RMSE_fullcf += Math.pow(fullcf_rating - real_rating, 2);
					N_fullcf++;
				}
				if (cross_rating > 0) {
					MAE_cross += Math.abs(cross_rating - real_rating);
					RMSE_cross += Math.pow(cross_rating - real_rating, 2);
					N_cross++;
				}
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		MAE_mib = MAE_mib / N_mib;
		RMSE_mib = Math.sqrt(RMSE_mib / N_mib);

		MAE_fullcf = MAE_fullcf / N_fullcf;
		RMSE_fullcf = Math.sqrt(RMSE_fullcf / N_fullcf);

		MAE_cross = MAE_cross / N_cross;
		RMSE_cross = Math.sqrt(RMSE_cross / N_cross);

		System.out.println();
		System.out.println("mib方法覆盖的那些用户：");
		System.out.println("MAE_mib=" + MAE_mib);
		System.out.println("MAE_cross=" + MAE_cross);
		System.out.println("MAE_fullcf=" + MAE_fullcf);
		System.out.println();
		System.out.println("RMSE_mib=" + RMSE_mib);
		System.out.println("RMSE_cross=" + RMSE_cross);
		System.out.println("RMSE_fullcf=" + RMSE_fullcf);

		MAE_mib = 0;
		RMSE_mib = 0;
		N_mib = 0;

		MAE_fullcf = 0;
		RMSE_fullcf = 0;
		N_fullcf = 0;

		MAE_cross = 0;
		RMSE_cross = 0;
		N_cross = 0;

		try {
			String sql = "select real_rating, mib_rating, fullcf_rating, cross_rating from "
					+ Tables.fromString("recommendation").getTableName() + " where cross_overlap=1";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				double real_rating = rs.getDouble(1) / 5;
				double mib_rating = rs.getDouble(2) / 5;
				double fullcf_rating = rs.getDouble(3) / 5;
				double cross_rating = rs.getDouble(4) / 5;

				if (mib_rating > 0) {
					MAE_mib += Math.abs(mib_rating - real_rating);
					RMSE_mib += Math.pow(mib_rating - real_rating, 2);
					N_mib++;
				}
				if (fullcf_rating > 0) {
					MAE_fullcf += Math.abs(fullcf_rating - real_rating);
					RMSE_fullcf += Math.pow(fullcf_rating - real_rating, 2);
					N_fullcf++;
				}
				if (cross_rating > 0) {
					MAE_cross += Math.abs(cross_rating - real_rating);
					RMSE_cross += Math.pow(cross_rating - real_rating, 2);
					N_cross++;
				}
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		MAE_mib = MAE_mib / N_mib;
		RMSE_mib = Math.sqrt(RMSE_mib / N_mib);

		MAE_fullcf = MAE_fullcf / N_fullcf;
		RMSE_fullcf = Math.sqrt(RMSE_fullcf / N_fullcf);

		MAE_cross = MAE_cross / N_cross;
		RMSE_cross = Math.sqrt(RMSE_cross / N_cross);

		System.out.println();
		System.out.println("cross方法覆盖的那些用户：");
		System.out.println("MAE_mib=" + MAE_mib);
		System.out.println("MAE_cross=" + MAE_cross);
		System.out.println("MAE_fullcf=" + MAE_fullcf);
		System.out.println();
		System.out.println("RMSE_mib=" + RMSE_mib);
		System.out.println("RMSE_cross=" + RMSE_cross);
		System.out.println("RMSE_fullcf=" + RMSE_fullcf);
	}

	public void showOverlapEffect() {
		System.out.println();
		int recommendation_times = 0;
		try {
			String sql = "select count(*) from "
					+ Tables.fromString("recommendation").getTableName();
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			if (rs.next()) {
				recommendation_times = rs.getInt(1);
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("覆盖率：");
		try {
			String sql = "select count(*) from "
					+ Tables.fromString("recommendation").getTableName()
					+ " where fullcf_overlap=1";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			if (rs.next()) {
				System.out.println("cf方法的覆盖率：" + rs.getInt(1) * 1.0
						/ recommendation_times);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			String sql = "select count(*) from "
					+ Tables.fromString("recommendation").getTableName() + " where mib_overlap=1";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			if (rs.next()) {
				System.out.println("mib方法的覆盖率：" + rs.getInt(1) * 1.0
						/ recommendation_times);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			String sql = "select count(*) from "
					+ Tables.fromString("recommendation").getTableName() + " where cross_overlap=1";
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			if (rs.next()) {
				System.out.println("cross方法的覆盖率：" + rs.getInt(1) * 1.0
						/ recommendation_times);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void showMAPAndNDCGEffect(int topN) {
		// 获得推荐结果集中的用户集合
		HashSet<String> userIdInTest = new HashSet<String>();
		try {
			String sql = "select distinct uid from "
					+ Tables.fromString("recommendation").getTableName();
			PreparedStatement pre = con.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				userIdInTest.add(rs.getString(1));
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Iterator<String> userIdInTestIter;

		double cf_MAP = 0;
		double cross_MAP = 0;
		double mib_MAP = 0;

		double cf_NDCG = 0;
		double cross_NDCG = 0;
		double mib_NDCG = 0;

		int userNumber = 0;

		userIdInTestIter = userIdInTest.iterator();
		while (userIdInTestIter.hasNext()) { // 对于每一个用户进行遍历
			// 获得实际的评分
			@SuppressWarnings("unchecked")
			HashSet<String> RatingPid[] = new HashSet[6]; // 桶，评分为下标的item集合
			int hasUsedNumber[] = new int[6]; // 记录每个桶中已经使用过的item数量
			for (int i = 1; i < 6; i++) {
				RatingPid[i] = new HashSet<String>();
			}
			String userId = (String) userIdInTestIter.next();
			int _topN = 0;
			// 获得该用户在验证集中的商品id及其实际评分，并放入相应桶中
			try {
				String sql = "select pid,real_rating from "
						+ Tables.fromString("recommendation").getTableName() + " where uid="
						+ userId;
				PreparedStatement pre = con.prepareCall(sql);
				ResultSet rs = pre.executeQuery();
				while (rs.next()) {
					RatingPid[rs.getInt(2)].add(rs.getString(1));
					_topN++;
				}
				rs.close();
				pre.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (_topN < topN) {
				continue;
			}

			// 计算cf方法的map
			for (int i = 1; i < 6; i++) {
				hasUsedNumber[i] = 0;
			}
			String cf_pid[] = new String[topN]; // 按推荐评分从大到小排列的item集合
			int cf_pid_number = 0;
			try {
				String sql = "select pid from "
						+ Tables.fromString("recommendation").getTableName()
						+ " where uid="
						+ userId
						+ " and mib_overlap>0 order by fullcf_rating desc limit "
						+ topN;
				PreparedStatement pre = con.prepareCall(sql);
				ResultSet rs = pre.executeQuery();
				while (rs.next()) {
					cf_pid[cf_pid_number++] = rs.getString(1);
				}
				rs.close();
				pre.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			double cf_AE = 0; // 单个用户的MAE
			for (int i = 0; i < cf_pid_number; i++) { // i+1即为预测评分的排序
				int index_in_real = 0; // 实际评分的排序
				for (int j = 5; j >= 1; j--) {
					if (RatingPid[j].contains(cf_pid[i])) {
						hasUsedNumber[j]++; // 相应桶中的计数加一
						index_in_real += hasUsedNumber[j];
						break;
					} else {
						index_in_real += RatingPid[j].size();
					}
				}
				if (i + 1 > index_in_real) { // 实际的下标与推测的下标之比即为单个的MAE
					cf_AE += index_in_real * 1.0 / (i + 1);
				} else {
					cf_AE += (i + 1) * 1.0 / index_in_real;
				}
			}
			cf_AE = cf_AE / topN;
			cf_MAP += cf_AE;
			cf_NDCG += getNDCG(cf_pid, RatingPid);
			// 计算cf方法的map结束

			// 计算cross方法的map
			for (int i = 1; i < 6; i++) {
				hasUsedNumber[i] = 0;
			}
			String cross_pid[] = new String[topN];
			int cross_pid_number = 0;
			try {
				String sql = "select pid from "
						+ Tables.fromString("recommendation").getTableName()
						+ " where uid="
						+ userId
						+ " and mib_overlap>0 order by cross_rating desc limit "
						+ topN;
				PreparedStatement pre = con.prepareCall(sql);
				ResultSet rs = pre.executeQuery();
				while (rs.next()) {
					cross_pid[cross_pid_number++] = rs.getString(1);
				}
				rs.close();
				pre.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			double cross_AE = 0;
			for (int i = 0; i < cross_pid_number; i++) {
				int index_in_real = 0;
				for (int j = 5; j >= 1; j--) {
					if (RatingPid[j].contains(cross_pid[i])) {
						hasUsedNumber[j]++;
						index_in_real += hasUsedNumber[j];
						break;
					} else {
						index_in_real += RatingPid[j].size();
					}
				}
				if (i + 1 > index_in_real) {
					cross_AE += index_in_real * 1.0 / (i + 1);
				} else {
					cross_AE += (i + 1) * 1.0 / index_in_real;
				}
			}
			cross_AE = cross_AE / topN;
			cross_MAP += cross_AE;
			cross_NDCG += getNDCG(cross_pid, RatingPid);
			// 计算cross方法的map结束

			// 计算mlb方法的map
			for (int i = 1; i < 6; i++) {
				hasUsedNumber[i] = 0;
			}
			String mib_pid[] = new String[topN];
			int mib_pid_number = 0;
			try {
				String sql = "select pid from "
						+ Tables.fromString("recommendation").getTableName() + " where uid="
						+ userId
						+ " and mib_overlap>0 order by mib_rating desc limit "
						+ topN;
				PreparedStatement pre = con.prepareCall(sql);
				ResultSet rs = pre.executeQuery();
				while (rs.next()) {
					mib_pid[mib_pid_number++] = rs.getString(1);
				}
				rs.close();
				pre.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			double mib_AE = 0;
			for (int i = 0; i < mib_pid_number; i++) {
				int index_in_real = 0;
				for (int j = 5; j >= 1; j--) {
					if (RatingPid[j].contains(mib_pid[i])) {
						hasUsedNumber[j]++;
						index_in_real += hasUsedNumber[j];
						break;
					} else {
						index_in_real += RatingPid[j].size();
					}
				}
				if (i + 1 > index_in_real) {
					mib_AE += index_in_real * 1.0 / (i + 1);
				} else {
					mib_AE += (i + 1) * 1.0 / index_in_real;
				}
			}
			mib_AE = mib_AE / topN;
			mib_MAP += mib_AE;
			mib_NDCG += getNDCG(mib_pid, RatingPid);
			// 计算mib方法的map结束

			userNumber++;
		}
		System.out.println("topN:" + topN);
		System.out.println("cf_MAP=" + cf_MAP / userNumber);
		System.out.println("cross_MAP=" + cross_MAP / userNumber);
		System.out.println("mib_MAP=" + mib_MAP / userNumber);
		System.out.println("cf_NDCG=" + cf_NDCG / userNumber);
		System.out.println("cross_NDCG=" + cross_NDCG / userNumber);
		System.out.println("mib_NDCG=" + mib_NDCG / userNumber);
	}

	/**
	 * 求ndcg
	 * 
	 * @param guess_pid
	 *            按预测评分从高到低的顺序排列的item数组
	 * @param RatingPid
	 *            按实际评分从高到底的循序排列的item桶数组，其中下标为评分，每个桶中的item的评分相同
	 * @return
	 */
	private double getNDCG(String[] guess_pid, HashSet<String> RatingPid[]) {
		double ndcg = 0;
		int number[] = new int[RatingPid.length];
		for (int i = 1; i < RatingPid.length; i++) {
			number[i] = 0; // 计算每个桶中使用过的item数量
		}
		for (int i = 0; i < guess_pid.length; i++) {
			for (int j = 5; j >= 1; j--) {
				if (RatingPid[j].contains(guess_pid[i])) {
					ndcg += (Math.pow(2, j) - 1) / Math.log(1 + i + 1)
							* Math.log(2); // 假设公式中的r(j)即为评分
					number[j]++;
				}
			}
		}
		double idcg = 0;
		int i = 0;
		for (int j = 5; j >= 1; j--) {
			while (number[j] > 0) {
				idcg += (Math.pow(2, j) - 1) / Math.log(1 + i + 1)
						* Math.log(2);
				i++;
				number[j]--;
			}
		}
		if (Math.abs(idcg) > Constants.DOUBLE_PRECISION) {
			ndcg = ndcg / idcg;
		}
		return ndcg;
	}
}
