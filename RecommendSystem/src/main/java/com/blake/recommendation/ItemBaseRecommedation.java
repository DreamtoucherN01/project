package com.blake.recommendation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.blake.database.generator.DatabaseOperation;
import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.Constants;
import com.blake.util.MyArray;

public class ItemBaseRecommedation extends Recommendation{

	public ItemBaseRecommedation(HashMapHarness hm, DatabaseOperation dbo) {
		
		super(hm, dbo);
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
			HashMap<Integer, HashSet<String>> groupUserIdHM = getGroupUserIdHM(levelGroupIdHMByGivenItem);
			// 获得评价过给定item的所有user，计算相似度时有用
			Integer[] userIdByGivenItem = hm.useridsInPid.get(Integer.valueOf(givenItemId)); 
			if(null == userIdByGivenItem || userIdByGivenItem.length == 0) {
				
				continue;
			}

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
				HashMap<Integer, Double> itemRatingHM = hm.uidPidRatingHMInTrain.get(Integer.valueOf(givenUserId)); 

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
	protected double recommendByCrossItemBase(HashMap<Integer, Double> itemRatingHM, String givenItemId,
			Integer[] userIdByGivenItem,HashMap<Integer, HashSet<Integer>> groupIdWithGivenUid,
			HashMap<Integer, HashSet<Integer>> groupIdWithGivenItemId,
			int level, HashMap<Integer, HashSet<String>> groupUserIdHM,String givenUserId) {
		
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
		
		long begin = System.currentTimeMillis();
		HashMap<Integer, Double> GroupIdGuessRatingHM = new HashMap<Integer, Double>();
		 
		for (int i = 0; i < groupIdWithGivenUidGivenItem.length; i++) {
			
			HashMap<Integer, Double> itemIdSimilarityHM = new HashMap<Integer, Double>();

			String[] itemsId = getItemIdHSByGroupId(groupIdWithGivenUidGivenItem[i]).toArray(template)[0].split(" ");
			String[] userIdArrInGroup = groupUserIdHM.get(groupIdWithGivenUidGivenItem[i]).toArray(template);
			Integer[] userIdInGroupByGivenItem = MyArray.intersect(userIdArrInGroup, userIdByGivenItem);

			//Decrease recommendation time
			int length = itemsId.length;
			if(length > 10000) {
				
				length = length / 10;
			}
			for (int j = 0; j < length; j++) {
				
				if((itemsId[j]).equals("")) {
					
					continue;
				}
				if (itemRatingHM.get(Integer.valueOf(itemsId[j])) != null) {
					
					 if(!itemsId[j].equals(givenItemId)){
						 
						 itemIdSimilarityHM.put(Integer.valueOf(itemsId[j]),getSimilarityWithGivenPidByPid(Integer.valueOf(itemsId[j])
								 ,userIdArrInGroup,userIdInGroupByGivenItem));
					 } else {
						 
						 itemIdSimilarityHM.put(Integer.valueOf(itemsId[j]), 1.0); // 每个item的相似度一样
					 }
				}
				double guessRating = recommendByCFItemBase(itemRatingHM , itemIdSimilarityHM);
				if (guessRating > 0) {
					
					GroupIdGuessRatingHM.put(groupIdWithGivenUidGivenItem[i] , new Double(guessRating));
				}
			}
		}
		System.out.println("1 "+ (System.currentTimeMillis() - begin));
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
			PreparedStatement pre = dbo.getCon().prepareCall(sql);
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
	 * 运用cf方法预测
	 * 
	 * @param itemIdSimilarityHM 所有给定用户购买的商品与给定商品的相似度
	 * @param itemRatingHM 给定用户购买的所有item及其评分
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected double recommendByCFItemBase(HashMap<Integer, Double> itemRatingHM,
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

}
