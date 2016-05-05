package com.blake.database.generator;

import java.util.HashMap;

public class DatabaseOperationHelper{

	HashMap<Integer, HashMap<Integer, Double>> uidPidRatingHMInTrain;
	
	public DatabaseOperationHelper(HashMap<Integer, HashMap<Integer, Double>> uidPidRatingHMInTrain) {
		
		this.uidPidRatingHMInTrain = uidPidRatingHMInTrain;
	}

	public HashMap<Integer, Integer> getUserTotalReviewByUserIdItemId(
			String[] usersIdArr, String[] itemsIdArr) {
		
		HashMap<Integer, Integer> userTotalReview = new HashMap<Integer, Integer>();
		if (itemsIdArr.length == 0 || usersIdArr.length == 0) {
			
			return userTotalReview;
		}
		for(String uid : usersIdArr) {
			
			int totalReview = 0;
			HashMap<Integer, Double> pidRating = uidPidRatingHMInTrain.get(Integer.valueOf(uid));
			if(null != pidRating) {
				
				for(String pid : itemsIdArr) {
					
					if(!pid.equals("") && null != pidRating.get(Integer.valueOf(pid))){
						
						totalReview++;
					}
				}
			} else {
				
				continue;
			}
			userTotalReview.put(new Integer(uid), new Integer(totalReview));
		}
		return userTotalReview;
	}
}
