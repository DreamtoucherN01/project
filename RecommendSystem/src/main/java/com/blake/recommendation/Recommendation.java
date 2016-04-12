package com.blake.recommendation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.blake.database.generator.DatabaseOperation;
import com.blake.database.generator.DatabaseOperationImpl;
import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.Constants;
import com.blake.util.MyArray;

public class Recommendation {
	
	protected DatabaseOperationImpl dbo;
	protected Connection conWorkspace;
	protected HashMapHarness hm;
	
	private Integer[] templateStr = {}; 
	private int pidInTestNumber = 0;
	private long cf_time = 0;
	private long cross_time = 0;
	private long mib_time = 0;
	private int recommendationNumber = 0; 

	public Recommendation(HashMapHarness hm, Connection conWorkspace, DatabaseOperation dbo) {
		
		this.hm = hm;
		this.conWorkspace = conWorkspace;
		this.dbo = (DatabaseOperationImpl) dbo;
	}
	
	/**
	 * 根据评论分割的方法对用户进行推荐,相似度按group内计算，对用户评价过的item进行评分预测，itembase
	 */
	public void doRecommendationByReviewSplitItemBase() {
		
		System.out.println(this.getClass().getName() + " doRecommendationByReviewSplitItemBase()");
		String[] pidInTestArr = new String[hm.pidUidRatingHMInTest.size()];
		
		Iterator<Integer> pidUidRatingIter = hm.pidUidRatingHMInTest.keySet().iterator();
		while (pidUidRatingIter.hasNext()) {
			
			pidInTestArr[pidInTestNumber++] =  String.valueOf(pidUidRatingIter.next());
		}
		
		long begin = System.currentTimeMillis();
		for (int pid = 0; pid < pidInTestNumber; pid++) {
			
			// 给定的商品id
			String givenItemId = pidInTestArr[pid]; 
			// 获得给定item关联group，这些group按层次分类
			String categoryStr = hm.itemCategoryStr.get(Integer.valueOf(givenItemId));
			if(null == categoryStr || categoryStr.length() == 0) {
				
				continue;
			}
			HashMap<Integer, HashSet<Integer>> levelGroupIdHMByGivenItem = getGroupIdByCategoryId(categoryStr.trim().split(" ")); 
			if(null == levelGroupIdHMByGivenItem || levelGroupIdHMByGivenItem.isEmpty()) {

				continue;
			}
			// 获得这些group对应的用户，分别为group和用户集合的键值对
			HashMap<Integer, HashSet<Integer>> groupUserIdHM = getGroupUserIdHM(levelGroupIdHMByGivenItem);
			// 获得评价过给定item的所有user，计算相似度时有用
			Integer[] userIdByGivenItem = hm.useridsInPid.get(Integer.valueOf(givenItemId)); 

			// 获得验证集中评价过给定商品的用户和评分的键值对
			HashMap<Integer, Double> userIdRatingInTest = hm.pidUidRatingHMInTest.get(Integer.valueOf(givenItemId));

			Iterator<Integer> userIdIter = userIdRatingInTest.keySet().iterator();
			while (userIdIter.hasNext()) {
				
				// 给定的userId
				String givenUserId = String.valueOf( userIdIter.next() ); 
				// 获得给定用户的关联group,这些group按层次分类
				HashMap<Integer, HashSet<Integer>> levelGroupIdHMByGivenUser = getLevelGroupIdHMByUid(givenUserId);
				if(null == levelGroupIdHMByGivenUser || levelGroupIdHMByGivenUser.isEmpty() ) {

					continue;
				}
				// 获得给定用户评价过的所有item及其评分
				HashMap<Integer, Double> itemRatingHM = hm.uidPidRatingHMInTest.get(Integer.valueOf(givenUserId)); 

				// 用CF方法推荐
				long cf_startTime = System.currentTimeMillis();
				HashMap<Integer, Double> itemIdSimilarityHM = new HashMap<Integer, Double>();
				Iterator<Integer> itemIter = itemRatingHM.keySet().iterator();
				while (itemIter.hasNext()) {

					Integer itemId = itemIter.next();
					if (!itemId.equals(givenItemId)) {
						
						itemIdSimilarityHM.put(itemId,getSimilarityInAllUser(itemId,userIdByGivenItem));
					}
				}
				int cf_overFlag = 1;
				double cf_guessRating = recommendByCFItemBase(itemRatingHM , itemIdSimilarityHM);
				if (cf_guessRating < 0) {
					
					cf_overFlag = 0;
				}
				cf_time += System.currentTimeMillis() - cf_startTime;

				// 用cross方法推荐
				long cross_startTime = System.currentTimeMillis();
				int cross_overFlag = 1;
				double cross_guessRating;
				
				if (cf_guessRating < 0) {
					
					// 注意到如果cf方法不能推荐，则cross一定不能推荐
					cross_guessRating = -1;
				} else {
					
					cross_guessRating = recommendByCrossItemBase(itemRatingHM,
							givenItemId, 
							userIdByGivenItem,
							levelGroupIdHMByGivenUser,
							levelGroupIdHMByGivenItem,
							Constants.CATEGORY_LEVEL_NUM - 1, 
							groupUserIdHM,
							givenUserId);
				}
				if (cross_guessRating < 0) {
					cross_guessRating = cf_guessRating;
					cross_overFlag = 0;
				}
				cross_time += System.currentTimeMillis() - cross_startTime;

				// 用mib方法推荐
				long mib_startTime = System.currentTimeMillis();
				int mib_overFlag = 1;
				double mib_guessRating = -1;
				if (cf_guessRating < 0) {
					
					// 注意到cf不能推荐，则mib一定不能推荐
					mib_guessRating = -1;
				} else {
					
					for (int j = Constants.CATEGORY_LEVEL_NUM - 1; j >= 0; j--) {
						mib_guessRating = recommendByCrossItemBase(
								itemRatingHM, 
								givenItemId, 
								userIdByGivenItem,
								levelGroupIdHMByGivenUser,
								levelGroupIdHMByGivenItem, 
								j, 
								groupUserIdHM,
								givenUserId);
						if (mib_guessRating > 0) {
							break;
						}
					}
				}
				if (mib_guessRating < 0) {
					
					mib_guessRating = cf_guessRating;
					mib_overFlag = 0;
				}
				mib_time += System.currentTimeMillis() - mib_startTime;

				Double real_rating = userIdRatingInTest.get(Integer.valueOf(givenUserId));
				if (real_rating == null) {
					
					real_rating = new Double(-1);
				}
				
				dbo.insertIntoRecommendation(givenItemId, givenUserId,real_rating.doubleValue(), 
						cf_guessRating, cf_overFlag,
						cross_guessRating, cross_overFlag, 
						mib_guessRating, mib_overFlag);
				recommendationNumber++;
			}
			if (recommendationNumber > Constants.RECOMMENDATION_TIMES) {
				
				break;
			}
		}
		System.out.println("recommendation done , time cost is " + (System.currentTimeMillis() - begin)/1000);
		System.out.println("cf_time=" + cf_time * 1.0 / recommendationNumber);
		System.out.println("cross_time=" + cross_time * 1.0 / recommendationNumber);
		System.out.println("mib_time=" + mib_time * 1.0 / recommendationNumber);
		System.out.println();
	}
	
	/**
	 * 用cross给定层次进行推荐
	 * 
	 * @param itemRatingHM  			//在训练集中给定用户评价过的item及其评分
	 * @param givenItemId				//给定商品的id
	 * @param userIdByGivenItem			//给定商品包含的用户集合
	 * @param groupIdWithGivenUid		//给定用户关联的group，其中为level和group的键值对
	 * @param groupIdWithGivenItemId	//给定商品关联的group，其中为level和group的键值对
	 * @param level						//层次
	 * @param groupUserIdHM				//每个group对应的user
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private double recommendByCrossItemBase(HashMap<Integer, Double> itemRatingHM, String givenItemId,
			Integer[] userIdByGivenItem,HashMap<Integer, HashSet<Integer>> groupIdWithGivenUid,
			HashMap<Integer, HashSet<Integer>> groupIdWithGivenItemId,
			int level, HashMap<Integer, HashSet<Integer>> groupUserIdHM,String givenUserId) {
		
		// 检查输入
		if (userIdByGivenItem.length == 0 || level < 0 || level > Constants.CATEGORY_LEVEL_NUM - 1) {
			
			return -1;
		}
		HashSet<Integer> groupIdWithGivenUidInLevel = groupIdWithGivenUid.get(new Integer(level));
		HashSet<Integer> groupIdWithGivenItemIdInLevel = groupIdWithGivenItemId.get(new Integer(level));
		if (groupIdWithGivenUidInLevel == null || groupIdWithGivenItemIdInLevel == null) {
			
			return -1;
		}
		Integer[] groupIdWithGivenUidGivenItem = MyArray.intersect(
				groupIdWithGivenUidInLevel.toArray(templateStr),
				groupIdWithGivenItemIdInLevel.toArray(templateStr));

		if (groupIdWithGivenUidGivenItem.length == 0) {
			
			return -1;
		}
		
		HashMap<Integer, Double> GroupIdGuessRatingHM = new HashMap<Integer, Double>();
		for (int i = 0; i < groupIdWithGivenUidGivenItem.length; i++) {
			
			HashMap<Integer, Double> itemIdSimilarityHM = new HashMap<Integer, Double>();

			Integer[] itemsId = getItemIdHSByGroupId(groupIdWithGivenUidGivenItem[i]).toArray(templateStr);
			Integer[] userIdArrInGroup = groupUserIdHM.get(groupIdWithGivenUidGivenItem[i]).toArray(templateStr);
			Integer[] userIdInGroupByGivenItem = MyArray.intersect(userIdArrInGroup, userIdByGivenItem);

			for (int j = 0; j < itemsId.length; j++) {
				
				if (itemRatingHM.get(itemsId[j]) != null) {
					
					 if(itemRatingHM.get(itemsId[j])!=null&&!itemsId[j].equals(givenItemId)){
						 
						 itemIdSimilarityHM.put(itemsId[j],getSimilarityWithGivenPidByPid(itemsId[j],userIdArrInGroup,userIdInGroupByGivenItem));
					 } else {
						 
						 itemIdSimilarityHM.put(itemsId[j], 1.0); // 每个item的相似度一样
					 }
				}
				double guessRating = recommendByCFItemBase(itemRatingHM , itemIdSimilarityHM);
				if (guessRating > 0) {
					
					GroupIdGuessRatingHM.put(groupIdWithGivenUidGivenItem[i] , new Double(guessRating));
				}
			}
		}
		// 获得importance
		double totalImportance = 0;
		HashMap<String, Double> groupIdImportance = new HashMap<String, Double>();
		try {
			
			String sql = "select group_id,importance from " + Tables.group_user.getTableName()  
					+ " where user_id=" + givenUserId 
					+ " and group_id in (" + groupIdWithGivenUidGivenItem[0];
			for (int i = 1; i < groupIdWithGivenUidGivenItem.length; i++) {
				
				sql += "," + groupIdWithGivenUidGivenItem[i];
			}
			sql += ")";
			PreparedStatement pre = conWorkspace.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				String groupId = rs.getString(1);
				Double importance = new Double(rs.getDouble(2));
				if (GroupIdGuessRatingHM.get(Integer.valueOf(groupId)) != null) {
					
					totalImportance += importance.doubleValue();
					groupIdImportance.put(groupId, importance);
				}
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		double cross_rating = 0;
		if (totalImportance > 0) {
			
			Iterator<?> groupIdIter = groupIdImportance.entrySet().iterator();
			while (groupIdIter.hasNext()) {
				
				Map.Entry<String, Double> entry = (Map.Entry<String, Double>) groupIdIter.next();
				String groupId = entry.getKey();
				double importance = entry.getValue().doubleValue();
				cross_rating += GroupIdGuessRatingHM.get(Integer.valueOf(groupId)).doubleValue() * importance;
			}
			cross_rating = cross_rating / totalImportance;
		} else {
			
			cross_rating = -1;
		}

		return cross_rating;
	}

	/**
	 * 获得商品pid与给定商品的相似度，基于group的相似度
	 * 
	 * @param itemsId //比较商品id
	 * @param userIdArrInGroup //group里面的用户id集合
	 * @param userIdInGroupByGivenItem //给定商品在group中的用户集合
	 * @return
	 */
	private double getSimilarityWithGivenPidByPid(Integer itemsId,
			Integer[] userIdArrInGroup, Integer[] userIdInGroupByGivenItem) {
		
		double similarity = 0;
		if (userIdArrInGroup.length == 0 || userIdInGroupByGivenItem.length == 0) {
			
			return similarity;
		}
		Integer[] uid = hm.useridsInPid.get(itemsId);
		if(null ==uid) {
			
			return similarity;
		}
		uid = MyArray.intersect(uid, userIdArrInGroup);
		Integer[] iter = MyArray.intersect(uid, userIdInGroupByGivenItem);
		Integer[] union = MyArray.union(uid, userIdInGroupByGivenItem);
		if (union.length == 0) {
			
			return similarity;
		} else {
			
			return iter.length * 1.0 / union.length;
		}
	}

	
	/**
	 * 获得group中所有的item
	 * 
	 * @param groupIdWithGivenUidGivenItem
	 * @return
	 */
	private HashSet<Integer> getItemIdHSByGroupId(Integer groupIdWithGivenUidGivenItem) {
		HashSet<Integer> itemIdHS = new HashSet<Integer>();
		try {
			
			String sql = "select itemsId from " + Tables.grouptable.getTableName() + " where id=" + groupIdWithGivenUidGivenItem;
			PreparedStatement pre = conWorkspace.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				String[] itemStrs = rs.getString(1).trim().split(" ");
				Integer[] itemsIds =  new Integer[ itemStrs.length];
				for(int j = 0 ; j < itemStrs.length ; j++) {
					
					itemsIds[j] = Integer.valueOf(itemStrs[j]);
				}
				itemIdHS.addAll(Arrays.asList(itemsIds));
				//itemIdHS.addAll(getItemIdByCategoryId(rs.getString(1).trim().split(" ")));
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return itemIdHS;
	}

	/**
	 * 获得包含指定category的所有group，按层次分类
	 * 
	 * @param categoryId
	 * @return
	 */
	private HashMap<Integer, HashSet<Integer>> getGroupIdByCategoryId(String[] categoryId) {
		
		HashMap<Integer, HashSet<Integer>> groupIdLevelHashMap = new HashMap<Integer, HashSet<Integer>>();
		if (categoryId.length == 0) {
			
			return groupIdLevelHashMap;
		}
		try {
			
			String sql = "select group_id, level from "
					+ Tables.group_category.getTableName() + ","
					+ Tables.grouptable.getTableName() 
					+ " where group_id=id and item_id in (" + categoryId[0];
			for (int i = 1; i < categoryId.length; i++) {
				sql += "," + categoryId[i];
			}
			sql += ")";
			PreparedStatement pre = conWorkspace.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				int groupId = rs.getInt(1);
				Integer level = new Integer(rs.getInt(2));
				HashSet<Integer> groupsId = groupIdLevelHashMap.get(level);
				if (groupsId == null) {
					
					groupsId = new HashSet<Integer>();
				}
				groupsId.add(groupId);
				groupIdLevelHashMap.put(level, groupsId);
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return groupIdLevelHashMap;
	}

	
	/**
	 * 获得group对应的user
	 * 
	 * @return
	 */
	private HashMap<Integer, HashSet<Integer>> getGroupUserIdHM(
			HashMap<Integer, HashSet<Integer>> levelGroupIdHM) {
		
		HashMap<Integer, HashSet<Integer>> groupUserHM = new HashMap<Integer, HashSet<Integer>>();
		Iterator<HashSet<Integer>> iter = levelGroupIdHM.values().iterator();
		
		while (iter.hasNext()) {
			
			HashSet<Integer> groupIdHS = (HashSet<Integer>) iter.next();
			Iterator<Integer> groupIdIter = groupIdHS.iterator();
			while (groupIdIter.hasNext()) {
				
				int groupId =  groupIdIter.next();
				try {
					
					String sql = "select usersId from " + Tables.grouptable.getTableName() 
							+ " where id=" + groupId;
					PreparedStatement pre = conWorkspace.prepareCall(sql);
					ResultSet rs = pre.executeQuery();
					if (rs.next()) {
						
						HashSet<Integer> userIdHS = new HashSet<Integer>();
						String[] usersIds = rs.getString(1).trim().split(" ");
						Integer users[] = new Integer[usersIds.length];
						for(int i = 0 ; i < usersIds.length ; i++ ) {
							
							users[i] = Integer.valueOf(usersIds[i]);
						}
						userIdHS.addAll(Arrays.asList(users));
						groupUserHM.put(groupId, userIdHS);
					}
					rs.close();
					pre.close();
				} catch (Exception e) {
					
					e.printStackTrace();
				}
			}
		}
		return groupUserHM;
	}
	
	/**
	 * 获得特定用户关联的group
	 * 
	 * @return 用户关联的level和groupid的键值对
	 */
	private HashMap<Integer, HashSet<Integer>> getLevelGroupIdHMByUid(String uid) {
		
		HashMap<Integer, HashSet<Integer>> levelGroupIdHM = new HashMap<Integer, HashSet<Integer>>();
		// 获得验证集中的所有用户
		try {
			
			String sql = "select " + Tables.group_user.getTableName() + ".group_id, "
					+ Tables.grouptable.getTableName() + ".level " + "from "
					+ Tables.group_user.getTableName() + ", " + Tables.grouptable.getTableName()
					+ " " + "where " + Tables.group_user.getTableName() + ".user_id="
					+ uid + " and " + Tables.group_user.getTableName() + ".group_id="
					+ Tables.grouptable.getTableName() + ".id";
			PreparedStatement pre = conWorkspace.prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				int groupId = rs.getInt(1);
				Integer level = new Integer(rs.getInt(2));
				HashSet<Integer> groupIdHS = levelGroupIdHM.get(level);
				if (groupIdHS == null) {
					
					groupIdHS = new HashSet<Integer>();
				}
				groupIdHS.add(groupId);
				levelGroupIdHM.put(level, groupIdHS);
			}
			rs.close();
			pre.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return levelGroupIdHM;
	}
	
	/**
	 * 运用cf方法预测
	 * 
	 * @param itemIdSimilarityHM 所有给定用户购买的商品与给定商品的相似度
	 * @param itemRatingHM 给定用户购买的所有item及其评分
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private double recommendByCFItemBase(HashMap<Integer, Double> itemRatingHM,
			HashMap<Integer, Double> itemIdSimilarityHM) {
		
		double totalSimilarity = 0;
		double cf_rating = 0;
		Iterator<?> iter = itemIdSimilarityHM.entrySet().iterator();
		while (iter.hasNext()) {
			
			Map.Entry<Integer, Double> entry = (Map.Entry<Integer, Double>) iter.next();
			int pid = entry.getKey();
			Double similarity = entry.getValue();
			totalSimilarity += similarity.doubleValue();
			cf_rating += itemRatingHM.get(pid).doubleValue() * similarity.doubleValue();
		}
		if (cf_rating > 0) {
			
			cf_rating = cf_rating / totalSimilarity;
		} else {
			
			cf_rating = -1;
		}
		return cf_rating;
	}

	/**
	 * 获得商品itemId与给定商品的相似度，基于所有的用户
	 * 
	 * @param uid   			给定用户的id
	 * @param givenUidItem    	给定用户购买过的所有商品id
	 * @return					Similarity
	 */
	private double getSimilarityInAllUser(Integer itemId, Integer[] userIdByGivenItem) {
		
		double similarity = 0;
		if (userIdByGivenItem.length == 0) {
			
			return similarity;
		}
		Integer[] uid = hm.useridsInPid.get(itemId);
		if( null == uid) {
			
			return similarity;
		}
		Integer[] iter = MyArray.intersect(uid, userIdByGivenItem);
		Integer[] union = MyArray.union(uid, userIdByGivenItem);
		if (union.length == 0) {
			
			return similarity;
		} else {
			
			return iter.length * 1.0 / union.length;
		}
	}

}
