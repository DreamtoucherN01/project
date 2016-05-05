package com.blake.recommendation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.blake.database.generator.DatabaseOperation;
import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.Constants;
import com.blake.util.MyArray;

public class UserBaseRecommendation extends Recommendation{

	public UserBaseRecommendation(HashMapHarness hm, DatabaseOperation dbo) {
		
		super(hm , dbo);
	}

    /**
     * 根据评论分割的方法对用户进行推荐,相似度按group内计算，对用户评价过的item进行评分预测，userbase
     */
    @SuppressWarnings("rawtypes")
	public void doRecommendationByReviewSplitUserBase(){
    	
        System.out.println(this.getClass().getName() + "doRecommendationByReviewSplitUserBase()");
        Integer[] uidInTestArr=new Integer[hm.uidPidRatingHMInTest.size()];
        int uidInTestNumber=0;
        Iterator uidPidRatingIter=hm.uidPidRatingHMInTest.keySet().iterator();
        while(uidPidRatingIter.hasNext()){
        	
            uidInTestArr[uidInTestNumber++] =  (Integer) uidPidRatingIter.next();
        }
        long cf_time=0;
        long cross_time=0;
        long mib_time=0;
        int recommendationNumber=0; //推荐的次数
        for(int user=0;user < uidInTestNumber;user++){
        	
        	//给定的用户
        	String givenUserId = String.valueOf(uidInTestArr[user]);  
        	//获得给定用户的关联group
            HashMap<Integer, HashSet<Integer>> levelGroupIdHM = getLevelGroupIdHMByUid(givenUserId);  
            //获得这些group对应的item
            HashMap<String,HashSet<String>> groupItemIdHM = getGroupItemHM(levelGroupIdHM);           
            //获得给定用户评价过的所有item，计算相似度时有用
            String[] itemIdByGivenUid = getPidInTrainByUid(givenUserId);                                      
            
            //获得验证集中给定用户购买过的商品评分
            HashMap<Integer, Double> itemIdRatingInTrain = hm.uidPidRatingHMInTrain
            		.get(Integer.valueOf(givenUserId));
            
            Iterator itemIdIter = itemIdRatingInTrain.keySet().iterator();
            while(itemIdIter.hasNext()) {
            	
            	//给定的itemId
            	String givenItemId = String.valueOf(itemIdIter.next());   
            	//获得给定item对应的group,这些group按层次分类
                HashMap<String,HashMap<Integer,HashSet<Integer>>> itemIdGroupIdHM = getItemGroup(givenItemId);   
                //HashMap<String,HashMap<String,Double>> itemUserRatingHM = getItemUserRating(givenItemId);       //获得给定item对应的用户及其评分
                
                //获得购买过givenItemId的所有用户及其评分
                HashMap<Integer,Double> uidRatingInTrainHashMap = hm.pidUidRatingHMInTrain.get(Integer.valueOf(givenItemId));
                if(null == uidRatingInTrainHashMap || uidRatingInTrainHashMap.isEmpty()) {
                	
                	continue;
                }
                        
                //用CF方法推荐
                long cf_startTime = System.currentTimeMillis();
                HashMap<String, Double> userIdSimilarityHM = new HashMap<String, Double>();  
                Iterator uidIter = uidRatingInTrainHashMap.keySet().iterator();
                while(uidIter.hasNext()) {
                	
                    String userId =  uidIter.next().toString();
                    if(!userId.equals(givenUserId)) {
                    	
                        userIdSimilarityHM.put(userId, getSimilarityInAllItem(userId, itemIdByGivenUid));
                    }
                }
                int cf_overFlag=1;
                double cf_guessRating = recommendByCF(uidRatingInTrainHashMap, userIdSimilarityHM, true);
                if(cf_guessRating < 0){
                	
                    cf_overFlag = 0;
                }
                cf_time += System.currentTimeMillis() - cf_startTime;
                //CF方法结束
                        
                //用cross方法推荐
                long cross_startTime = System.currentTimeMillis();
                int cross_overFlag = 1;
                double cross_guessRating;
                //注意到如果cf方法不能推荐，则cross一定不能推荐
                if(cf_guessRating<0) {
                	
                    cross_guessRating = -1;
                } else {
                	
                    cross_guessRating = recommendByCross(
                    		uidRatingInTrainHashMap,
                    		givenUserId,
                    		itemIdByGivenUid,
                    		levelGroupIdHM,
                    		itemIdGroupIdHM.get(givenItemId),
                    		Constants.CATEGORY_LEVEL_NUM - 1,
                    		groupItemIdHM);
                }
                if(cross_guessRating < 0) {
                	
                    cross_guessRating = cf_guessRating;
                    cross_overFlag  = 0;
                }
                cross_time += System.currentTimeMillis() - cross_startTime;
                //cross方法结束
                
                //用mib方法推荐
                long mib_startTime = System.currentTimeMillis();
                int mib_overFlag = 1;
                double mib_guessRating = -1;
                //注意到cf不能推荐，则mib一定不能推荐
                if(cf_guessRating < 0) {
                	
                    mib_guessRating = -1;
                }else{
                	
                    for(int j = Constants.CATEGORY_LEVEL_NUM - 1;j >= 0; j--){
                        mib_guessRating = recommendByCross (
                        		uidRatingInTrainHashMap,
                        		givenUserId,
                        		itemIdByGivenUid,
                        		levelGroupIdHM,
                        		itemIdGroupIdHM.get(givenItemId),
                        		j,
                        		groupItemIdHM);
                        if(mib_guessRating > 0) {
                        	
                            break;
                        }
                    }
                }
                if(mib_guessRating < 0) {
                	
                    mib_guessRating = cf_guessRating;
                    mib_overFlag = 0;
                }
                mib_time += System.currentTimeMillis() - mib_startTime;
                //mib方法结束
                        
                Double real_rating = itemIdRatingInTrain.get(Integer.parseInt(givenItemId));
                if(real_rating == null) {
                	
                    real_rating = new Double(-1);
                }
                dbo.insertIntoRecommendation ( 
                		givenItemId,
                		givenUserId,
                		real_rating.doubleValue(),
                		cf_guessRating,
                		cf_overFlag,
                		cross_guessRating,
                		cross_overFlag,
                		mib_guessRating,
                		mib_overFlag);
                recommendationNumber++;
            }
            if(recommendationNumber > Constants.RECOMMENDATION_TIMES) {
            	
                break;
            }
        }
        System.out.println("cf_time = " + cf_time * 1.0 / recommendationNumber);
        System.out.println("cross_time = " + cross_time * 1.0 / recommendationNumber);
        System.out.println("mib_time = " + mib_time * 1.0 / recommendationNumber);
        System.out.println();
    }
    
    /**
     * 用cross给定层次进行推荐
     * @param uidRatingInTrainHashMap   //在训练集中对给定商品评价过的用户及其评分
     * @param givenUserId               //给定用户的id
     * @param itemIdByGivenUid          //给定用户购买过的商品集合
     * @param levelGroupIdHM       //给定用户关联的group，其中为level和group的键值对
     * @param hashMap    //给定商品关联的group，其中为level和group的键值对
     * @param level                     //层次
     * @param groupIdItemIdHM           //每个group对应的item
     * @return 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private double recommendByCross(HashMap<Integer, Double> uidRatingInTrainHashMap, String givenUserId, String[] itemIdByGivenUid,
                                    HashMap<Integer, HashSet<Integer>> levelGroupIdHM, HashMap<Integer, HashSet<Integer>> hashMap,
                                    int level, HashMap<String, HashSet<String>> groupIdItemIdHM){
        //检查输入
        if(itemIdByGivenUid.length == 0 || level < 0 || level > Constants.CATEGORY_LEVEL_NUM - 1) {
        	
            return -1;
        }
        HashSet<Integer> groupIdWithGivenUidInLevel = levelGroupIdHM.get(new Integer(level));
        HashSet<Integer> groupIdWithGivenItemIdInLevel = hashMap.get(new Integer(level));
        if(groupIdWithGivenUidInLevel == null || groupIdWithGivenItemIdInLevel == null){
        	
            return -1;
        }
        Integer[] groupIdWithGivenUidGivenItem = MyArray.intersect(
        		groupIdWithGivenUidInLevel.toArray(templateStr), groupIdWithGivenItemIdInLevel.toArray(templateStr));
        if(groupIdWithGivenUidGivenItem.length == 0) {
        	
            return -1;
        }
        HashMap<Integer, Double> GroupIdGuessRatingHM = new HashMap<Integer, Double>();
        HashMap<Integer, Integer> GroupIdGUserLengthHM = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> GroupIdratingLengthHM = new HashMap<Integer, Integer>();
        int uidLength = 0;
        for(int i=0;i<groupIdWithGivenUidGivenItem.length;i++) {
        	
        	int ratingLength = 0;
            HashMap<String, Double> userIdSimilarityHM = new HashMap<String, Double>();
            String[] usersId = getUidByGroupId(groupIdWithGivenUidGivenItem[i]);
            uidLength += usersId.length;
            if(!groupIdItemIdHM.containsKey(groupIdWithGivenUidGivenItem[i].toString())) {
            	
            	continue;
            }
            String[] itemIdArrInGroup = (groupIdItemIdHM.get(groupIdWithGivenUidGivenItem[i].toString())).toArray(template);
            String[] itemIdInGroupByGivenUid = MyArray.intersect(itemIdArrInGroup[0].split(" "), itemIdByGivenUid);
            if(null == itemIdInGroupByGivenUid || itemIdInGroupByGivenUid.length == 0) {
            	
            	continue;
            }
            
            for(int j=0;j<usersId.length;j++) {
            	
                if(!usersId[j].equals(givenUserId)){
                	
                    //userIdSimilarityHM.put(usersId[j], getSimilarityWithGivenUidByUid(usersId[j],itemIdArrInGroup,itemIdInGroupByGivenUid));
                    userIdSimilarityHM.put(usersId[j], getSimilarityInAllItem(usersId[j],itemIdByGivenUid));  //按item计算相似度
                    
                } else {
                	
                	userIdSimilarityHM.put(usersId[j], 1.0);    //每个用户的相似度一样
                }
                ratingLength += hm.uidPidRatingHMInTrain.get(Integer.valueOf(usersId[j])).size();
            }
            double guessRating = recommendByCF(uidRatingInTrainHashMap, userIdSimilarityHM, Constants.WEIGHTNEEDCALCULATED);
            if(guessRating > 0) {
            	
                GroupIdGuessRatingHM.put(groupIdWithGivenUidGivenItem[i], guessRating);
                GroupIdGUserLengthHM.put(groupIdWithGivenUidGivenItem[i], usersId.length);
                GroupIdratingLengthHM.put(groupIdWithGivenUidGivenItem[i], ratingLength);
            }
        }
        
        double cross_rating = 0;
        if(Constants.IMPORTANCEINCLUDED) {
        	
        	double totalImportance = 0;
        	double importance = 0;
    		//获得importance
	        HashMap<String,Double> groupIdImportance = new HashMap<String,Double>();
	        try{
	            String sql="select group_id,importance from " + Tables.group_user.getTableName()
	            		+ " where user_id=" + givenUserId + " and group_id in (" + groupIdWithGivenUidGivenItem[0];
	            for(int i=1; i<groupIdWithGivenUidGivenItem.length; i++) {
	            	
	                sql += "," + groupIdWithGivenUidGivenItem[i];
	            }
	            sql+=")";
	            PreparedStatement pre = dbo.getCon().prepareCall(sql);
	            ResultSet rs = pre.executeQuery();
	            while(rs.next()) {
	            	
	                Integer groupId = rs.getInt(1);
	                importance = new Double(rs.getDouble(2));
	                if(GroupIdGuessRatingHM.get(groupId) != null){
	                	
	                    totalImportance += importance;
	                    groupIdImportance.put(groupId.toString(), importance);
	                }
	            }
	            rs.close();
	            pre.close();
	        }catch(Exception e) {
	        	
	            e.printStackTrace();
	        }
	        if(totalImportance > 0){
	        	
	            Iterator groupIdIter = groupIdImportance.entrySet().iterator();
	            while(groupIdIter.hasNext()) {
	            	
	                Map.Entry<String,Double> entry = (Map.Entry<String,Double>) groupIdIter.next();
	                String groupId = entry.getKey();
	                importance = entry.getValue().doubleValue();
	                cross_rating += GroupIdGuessRatingHM.get(Integer.valueOf(groupId)).doubleValue() * importance;
	            }
	            cross_rating = cross_rating / totalImportance;
	        } else {
	        	
	            cross_rating = -1;
	        }
	        return cross_rating;
	        
        } else {
        	
        	Iterator it = GroupIdGuessRatingHM.entrySet().iterator();
        	double totalPropation = 0;
        	while(it.hasNext()) {
        		
        		Entry<Integer,Double> entry = (Entry<Integer,Double>) it.next();
        		int groupid = entry.getKey();
        		int propation = GroupIdGUserLengthHM.get(groupid) / uidLength 
        				+ hm.uidPidRatingHMInTrain.get(Integer.valueOf(givenUserId)).size() 
        					/ GroupIdratingLengthHM.get(groupid);
        		totalPropation += propation;
        		cross_rating += entry.getValue().doubleValue() * propation;
        	}
        	return cross_rating / totalPropation;
        }
        
        
    }
    
    /**
     * 运用cf方法预测
     * @param userIdSimilarityHM    所有购买指定商品的用户与给定用户的相似度,基于group的相似度
     * @param uidRatingInTrainHashMap   所有购买指定商品的用户对商品的评分
     * @return 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private double recommendByCF(HashMap<Integer, Double> uidRatingInTrainHashMap, HashMap<String,Double> userIdSimilarityHM) {
       
    	double cf_rating = 0; int uidNum = 0;
        Iterator iter = userIdSimilarityHM.entrySet().iterator();
        while(iter.hasNext()) {
        	
            Map.Entry<String,Double> entry = (Map.Entry<String,Double>) iter.next(); 
            String uid = entry.getKey();
            if(!uidRatingInTrainHashMap.containsKey(Integer.valueOf(uid))) {
            	
            	continue;
            }
            cf_rating += uidRatingInTrainHashMap.get(Integer.valueOf(uid)).doubleValue();
            uidNum++ ;
        }
        
        if(cf_rating > 0) {
        	
            cf_rating = cf_rating / uidNum ;
        }else{
        	
            cf_rating = -1;
        }
        return cf_rating;
    }
    
    /**
     * 运用cf方法预测
     * @param userIdSimilarityHM    所有购买指定商品的用户与给定用户的相似度，基于item的相似度
     * @param uidRatingInTrainHashMap   所有购买指定商品的用户对商品的评分
     * @return 
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	private double recommendByCF(HashMap<Integer, Double> uidRatingInTrainHashMap, HashMap<String,Double> userIdSimilarityHM, boolean itemFlag){
       
    	if(itemFlag) {
        	
            return recommendByCF(uidRatingInTrainHashMap,userIdSimilarityHM);
        }
        double totalSimilarity = 0;
        double cf_rating = 0;
        Iterator iter = userIdSimilarityHM.entrySet().iterator();
        while(iter.hasNext()){
        	
            Map.Entry<String,Double> entry = (Map.Entry<String,Double>) iter.next(); 
            String uid = entry.getKey();
            Double similarity = entry.getValue();
            if(!uidRatingInTrainHashMap.containsKey(Integer.valueOf(uid))) {
            	
            	continue;
            }
            totalSimilarity += similarity.doubleValue();
            cf_rating += uidRatingInTrainHashMap.get(
            		Integer.valueOf(uid)).doubleValue() * similarity.doubleValue();
        }
        if(cf_rating > 0) {
        	
            cf_rating = cf_rating / totalSimilarity;
        } else {
        	
            cf_rating = -1;
        }
        return cf_rating;
    }
}
