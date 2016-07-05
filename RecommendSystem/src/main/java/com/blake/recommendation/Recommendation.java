package com.blake.recommendation;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.blake.database.generator.DatabaseOperation;
import com.blake.database.generator.DatabaseOperationImpl;
import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.MyArray;

public class Recommendation {
	
	protected DatabaseOperationImpl dbo;
	protected HashMapHarness hm;
	
	protected Integer[] templateStr = {}; 
	protected String[] template = {}; 
	protected int pidInTestNumber = 0;
	protected long cf_time = 0;
	protected long cross_time = 0;
	protected long mib_time = 0;
	protected int recommendationNumber = 0; 

	public Recommendation(HashMapHarness hm, DatabaseOperation dbo) {
		
		this.hm = hm;
		this.dbo = (DatabaseOperationImpl) dbo;
		dbo.truncateTables(Tables.recommendation);
	}
	
	protected String[] getUidByGroupId(int groupId){
		
        String[] uidArr={};
        try{
        	
            String sql = "select usersId from " + Tables.grouptable.getTableName() + " where id = " + groupId;
            PreparedStatement pre = dbo.getCon().prepareCall(sql);
            ResultSet rs = pre.executeQuery();
            if(rs.next()){
            	
                uidArr = rs.getString(1).trim().split(" ");
            }
            rs.close();
            pre.close();
        }catch(Exception e) {
        	
            e.printStackTrace();
        }
        return uidArr;
    }
	
    /**
     * 获得item对应的group，其中group以level分类
     * @param givenItemId
     * @return 
     */
	protected HashMap<String,HashMap<Integer,HashSet<Integer>>> getItemGroup(
			String givenItemId){
        
    	HashMap<String,HashMap<Integer,HashSet<Integer>>> itemGroupHM = new HashMap<String,HashMap<Integer,HashSet<Integer>>>();
    	itemGroupHM.put(givenItemId, getGroupIdByCategoryId(hm.itemCategoryStr.get(
    			Integer.valueOf(givenItemId)).trim().split(" ")));
        return itemGroupHM;
    }
	
    /**
     * 获得item对应的用户评分
     * @param givenItemId
     * @return 
     */
	protected HashMap<String,HashMap<Integer,Double>> getItemUserRating(String givenItemId){
		
        HashMap<String,HashMap<Integer,Double>> itemUserRatingHM=new HashMap<String,HashMap<Integer,Double>>();
        itemUserRatingHM.put(String.valueOf(givenItemId), getUidRatingInTrainByPid(givenItemId));
        
        return itemUserRatingHM;
    }
	
    /**
     * 获得购买过pid的所有用户及其评分的键值对
     * @param pid   //商品id
     * @return 
     */
    private HashMap<Integer, Double> getUidRatingInTrainByPid(String pid){

        return hm.pidUidRatingHMInTrain.get(Integer.valueOf(pid));
    }
	
    /**
     * 获得用户uid在训练集合购买过的商品id集合
     * @param uid
     * @return 
     */
	@SuppressWarnings("rawtypes")
	protected String[] getPidInTrainByUid(String uid) {
		
        HashSet<String> pidHS=new HashSet<String>();

    	HashMap pidRating = hm.uidPidRatingHMInTrain.get(Integer.valueOf(uid));
    	if(null == pidRating){
    		
    		return new String[0];
    	}
    	Iterator it = pidRating.keySet().iterator();
        while(it.hasNext()){
        	
            pidHS.add(it.next().toString());
        }
        return pidHS.toArray(template);
    }

	/**
	 * 获得group中所有的item
	 * 
	 * @param groupIdWithGivenUidGivenItem
	 * @return
	 */
	protected HashSet<String> getItemIdHSByGroupId(Integer groupIdWithGivenUidGivenItem) {
		
		HashSet<String> itemIdHS = new HashSet<String>();
//		itemIdHS.addAll(Arrays.asList(hm.groupIdItemIds.get(groupIdWithGivenUidGivenItem).trim()));
		try {
			String sql = "select itemsId from " + Tables.grouptable.getTableName()
					+ " where id = " + groupIdWithGivenUidGivenItem;
			PreparedStatement pre = dbo.getCon().prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				String itemsId = rs.getString(1);
				itemIdHS.add(itemsId);
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
	protected HashMap<Integer, HashSet<Integer>> getGroupIdByCategoryId(String[] categoryId) {
		
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
			PreparedStatement pre = dbo.getCon().prepareCall(sql);
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
	protected HashMap<Integer, HashSet<String>> getGroupUserIdHM(
			HashMap<Integer, HashSet<Integer>> levelGroupIdHM) {
		
		HashMap<Integer, HashSet<String>> groupUserHM = new HashMap<Integer, HashSet<String>>();
		Iterator<HashSet<Integer>> iter = levelGroupIdHM.values().iterator();
		
		while (iter.hasNext()) {
			
			HashSet<Integer> groupIdHS = (HashSet<Integer>) iter.next();
			Iterator<Integer> groupIdIter = groupIdHS.iterator();
			while (groupIdIter.hasNext()) {
				
				int groupId =  groupIdIter.next();
				try {
					
					String sql = "select usersId from " + Tables.grouptable.getTableName() 
							+ " where id=" + groupId;
					PreparedStatement pre = dbo.getCon().prepareCall(sql);
					ResultSet rs = pre.executeQuery();
					if (rs.next()) {
						
						HashSet<String> userIdHS = new HashSet<String>();
						userIdHS.addAll(Arrays.asList(rs.getString(1).trim().split(" ")));
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
     * 获得group对应的item
     * @return 
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	protected HashMap<String,HashSet<String>> getGroupItemHM(HashMap<Integer, HashSet<Integer>> levelGroupIdHM){
    	
        HashMap<String,HashSet<String>> groupItemHM = new HashMap<String,HashSet<String>>();
        Iterator iter = levelGroupIdHM.values().iterator();
        while(iter.hasNext()){
        	
            HashSet<Integer> groupIdHS = (HashSet<Integer>)iter.next();
            Iterator groupIdIter = groupIdHS.iterator();
            while(groupIdIter.hasNext()){
            	
            	Integer groupId = (Integer) groupIdIter.next();
                HashSet<String> itemHS = groupItemHM.get(String.valueOf(groupId));
                if(itemHS == null){
                	
                    itemHS = new HashSet();
                }
                itemHS.addAll(getItemIdHSByGroupId(Integer.valueOf(groupId)));
                groupItemHM.put(String.valueOf(groupId), itemHS);
            }
        }
        return groupItemHM;
    }
	
	/**
	 * 获得特定用户关联的group
	 * 
	 * @return 用户关联的level和groupid的键值对
	 */
	protected HashMap<Integer, HashSet<Integer>> getLevelGroupIdHMByUid(String uid) {
		
		HashMap<Integer, HashSet<Integer>> levelGroupIdHM = new HashMap<Integer, HashSet<Integer>>();
		// 获得验证集中的所有用户
		try {
			
			String sql = "select " + Tables.group_user.getTableName() + ".group_id, "
					+ Tables.grouptable.getTableName() + ".level " + "from "
					+ Tables.group_user.getTableName() + ", " + Tables.grouptable.getTableName()
					+ " " + "where " + Tables.group_user.getTableName() + ".user_id="
					+ uid + " and " + Tables.group_user.getTableName() + ".group_id="
					+ Tables.grouptable.getTableName() + ".id";
			PreparedStatement pre = dbo.getCon().prepareCall(sql);
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
	 * 获得商品itemId与给定商品的相似度，基于所有的用户
	 * 
	 * @param uid   			给定用户的id
	 * @param givenUidItem    	给定用户购买过的所有商品id
	 * @return					Similarity
	 */
	protected double getSimilarityInAllUser(Integer itemId, Integer[] userIdByGivenItem) {
		
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
	
    /**
     * 获得用户uid与给定用户的相似度，基于所有的item
     * @param uid   //给定用户的id
     * @param givenUidItem  //给定用户购买过的所有商品id
     * @return 
     */
	protected double getSimilarityInAllItem(String uid, String[] givenUidItem){
		
        double similarity = 0;
        if(givenUidItem.length == 0){
        	
            return similarity;
        }
        String[] pid = getPidInTrainByUid(uid);
        String[] iter = MyArray.intersect(pid, givenUidItem);
        String[] union = MyArray.union(pid, givenUidItem);
        if(union.length == 0){
        	
            return similarity;
        }else{
        	
            return iter.length*1.0/union.length;
        }
    }
	
    /**
     * 获得用户uid与给定用户的相似度，基于group的相似度
     * 
     * @param uid   //比较用户id
     * @param itemIdInGroup //group里面的itemid集合
     * @param givenUidItem  //给定用户在groupid中购买过的商品id
     * @return 
     */
	protected double getSimilarityWithGivenUidByUid(String uid, String[] itemIdInGroup, String[] givenUidItem) {
		
        double similarity = 0;
        if(itemIdInGroup.length == 0 || givenUidItem.length == 0){
        	
            return similarity;
        }
        String[] pid1 = getPidInTrainByUid(uid);
        pid1 = MyArray.intersect(pid1, itemIdInGroup);
        String[] iter = MyArray.intersect(pid1, givenUidItem);
        String[] union = MyArray.union(pid1, givenUidItem);
        if(union.length == 0){
        	
            return similarity;
        } else {
        	
            return iter.length * 1.0 / union.length;
        }
    }
	
	/**
	 * 获得商品pid与给定商品的相似度，基于group的相似度
	 * 
	 * @param itemsId //比较商品id
	 * @param userIdArrInGroup //group里面的用户id集合
	 * @param userIdInGroupByGivenItem //给定商品在group中的用户集合
	 * @return
	 */
	protected double getSimilarityWithGivenPidByPid(Integer itemsId,
			String[] userIdArrInGroup, Integer[] userIdInGroupByGivenItem) {
		
		double similarity = 0;
		if (userIdArrInGroup.length == 0 || userIdInGroupByGivenItem.length == 0) {
			
			return similarity;
		}
		Integer[] uid = hm.useridsInPid.get(itemsId);
		if(null ==uid) {
			
			return similarity;
		}
		uid = MyArray.intersect(userIdArrInGroup, uid);
		Integer[] iter = MyArray.intersect(uid, userIdInGroupByGivenItem);
		Integer[] union = MyArray.union(uid, userIdInGroupByGivenItem);
		if (union.length == 0) {
			
			return similarity;
		} else {
			
			return iter.length * 1.0 / union.length;
		}
	}
}
