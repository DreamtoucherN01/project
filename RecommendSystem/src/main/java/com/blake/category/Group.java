package com.blake.category;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.blake.database.generator.DatabaseOperation;
import com.blake.database.generator.DatabaseOperationImpl;
import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.Constants;
import com.blake.util.Functions;
import com.blake.util.MyArray;

public class Group {

	protected DatabaseOperationImpl dbo;
	protected HashMapHarness hm;
	
	private int groupId = 1;
	
	public Group(HashMapHarness hm, DatabaseOperation dbo) {
		
		this.hm = hm;
		this.dbo = (DatabaseOperationImpl) dbo;
		dbo.truncateTables(Tables.group_category);
		dbo.truncateTables(Tables.group_user);
		dbo.truncateTables(Tables.grouptable);
	}
	
	public void mineData(){
		
		System.out.println(this.getClass().getName() + " mineData");
		
		if(Constants.IS_USER_BASE) {
			
			UserBasedInput ubi = new UserBasedInput(hm, dbo.getCon());
			ubi.getGroupMiningInput();
		} else {
			
			ItemBasedInput ibi = new ItemBasedInput(hm, dbo.getCon());
			ibi.getGroupMiningInput();
		}
		groupMining();
	}

	/**
	 * 多层次挖掘
	 */
	@SuppressWarnings("unchecked")
	private void groupMining() {
		
		System.out.println(this.getClass().getName() + " groupMining");
		HashMap<Integer, String> categoryHashMap[] = new HashMap[Constants.CATEGORY_LEVEL_NUM];// categoryId与该category的父节点
		HashMap<Integer, Integer> categoryIdLevelHashMap = new HashMap<Integer, Integer>(); // 记录categoryId,categoryId与层次的键值对
		for (int i = 0; i < Constants.CATEGORY_LEVEL_NUM; i++) {
			
			categoryHashMap[i] = new HashMap<Integer, String>();
		}
		try {
			
			String sql = "select categoryparent.category,level,parent from categoryparent,levelcategory "
					+ "where categoryparent.category = levelcategory.category order by level";
			
			PreparedStatement pre = dbo.getCon().prepareCall(sql);
			ResultSet rs = pre.executeQuery();
			while (rs.next()) {
				
				int categoryId = rs.getInt(1);
				int level = rs.getInt(2);
				int parent = rs.getInt(3);
				categoryHashMap[level].put(categoryId, String.valueOf(parent));
				categoryIdLevelHashMap.put(categoryId,level);
			}
			rs.close();
			pre.close();
			pre = null;
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		HashSet<Integer> filterSet = new HashSet<Integer>(); // 记录所有已经覆盖过的category
		String file_separator = System.getProperty("file.separator");
		for (int i = Constants.CATEGORY_LEVEL_NUM - 1; i > 0; i--) {
			
			System.out.println(this.getClass().getName() + " mine level " + i);
			
			HashSet<Integer> leftCategory = filterCategory(filterSet , categoryHashMap[i]);
			if (leftCategory.isEmpty()) {
				
				continue;
			}
			
			int num = getCateLevel(i);
			
			Functions.openExe(Constants.PATH + "/exe/rpmine"+num+".exe "+ Constants.PATH +"/data/gen"+ file_separator
							+ "groupInputWithoutUser_"+ i+ ".dat "
							+ (Constants.BUTTOM_MIN_SUP / Math.pow(
									Constants.MIN_SUP_LOSS_RATE,
									Constants.CATEGORY_LEVEL_NUM - 1 - i))
							+ " " + Constants.DELTA + " " + Constants.PATH + "/data/out" + file_separator
							+ "groupOutput_" + i + ".dat", true);
			
			getGroupAndAddFilter(filterSet, i, categoryHashMap[i], leftCategory, categoryIdLevelHashMap);
		}
		for (int i = 0; i < Constants.CATEGORY_LEVEL_NUM; i++) {
			
			categoryHashMap[i] = null;
		}
		categoryIdLevelHashMap = null;
		System.gc();
	}
	
	@SuppressWarnings("unchecked")
	private void getGroupAndAddFilter(HashSet<Integer> filterSet, int level,
			HashMap<Integer, String> categoryHashMap,
			HashSet<Integer> leftCategory,
			HashMap<Integer, Integer> categoryIdLevelHashMap) {
		
		System.out.println(this.getClass().getName() + " filter level " + level);
		long begin = System.currentTimeMillis();
		String file_separator = System.getProperty("file.separator");
		HashMap<Integer, String[]> userData = new HashMap<Integer, String[]>(); // 用户id和用户数据键值对
		try {
			
			File file = new File(Constants.PATH + "/data/gen" + file_separator + "groupInputWithUser_" + level + ".dat");
			FileReader fr = new FileReader(file);
			BufferedReader reader = new BufferedReader(fr);
			String line;
			while ((line = reader.readLine()) != null) {
				
				String lineArr[] = line.split(" ", 2);
				if (lineArr.length < 2) {
					
					continue;
				}
				userData.put(new Integer(lineArr[0]), lineArr[1].split(" "));
			}
			reader.close();
			fr.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		try {
			
			File file = new File(Constants.PATH + "/data/out" + file_separator + "groupOutput_" + level + ".dat");
			FileReader fr = new FileReader(file);
			BufferedReader reader = new BufferedReader(fr);
			String line;
			while ((line = reader.readLine()) != null) {
				
				String items = line.substring(0, line.indexOf("("));
				if (items.trim().equals("")) {
					
					continue;
				}
				int support = Integer.parseInt(line.substring(line.indexOf("(") + 1, line.indexOf(")")));
				String itemsArr[] = items.split(" ");
				
				if (itemsArr.length > 1 && haveUncoveredCategory(itemsArr, leftCategory)) { 
					
					for (int i = 0; i < itemsArr.length; i++) {
						
						filterSet.add(new Integer(itemsArr[i]));
						String parentsId = categoryHashMap.get(new Integer(itemsArr[i]));
						if (parentsId == null || parentsId.equals("")) {
							
							continue;
						}
						String parentsIdArr[] = parentsId.split(" ");
						for (int j = 0; j < parentsIdArr.length; j++) {
							
							filterSet.add(new Integer(parentsIdArr[j]));
						}
					}
					
					// 获得包含用户
					StringBuilder usersIdSB = new StringBuilder("");
					Iterator<?> iter = userData.entrySet().iterator();
					while (iter.hasNext()) {
						
						Map.Entry<Integer, String[]> entry = (Map.Entry<Integer, String[]>) iter.next();
						
//						if (MyArray.isSimilarContain(itemsArr, entry.getValue())) { 
						if (MyArray.isContain(itemsArr, entry.getValue())) { 
							
							usersIdSB.append(entry.getKey().intValue()).append(" ");
						}
					}
//					System.out.println(" user length " + usersIdSB.length() + " detail " + usersIdSB);
					// 计算item与整个group的sim
					// 记录itemsArr中任何两个的相似度，下标是itemsArr中的下标
					double simArr[][] = new double[itemsArr.length][itemsArr.length]; 
					for (int i = 0; i < itemsArr.length; i++) {
						
						for (int j = i; j < itemsArr.length; j++) {
							
							if (i == j) {
								
								simArr[i][j] = 1.0;
							} else {
								
								String parentsIdi = categoryHashMap.get(new Integer(itemsArr[i]));
								String parentsIdj = categoryHashMap.get(new Integer(itemsArr[j]));
								if (parentsIdi == null || parentsIdj == null
										|| parentsIdi.equals("") || parentsIdj.equals("")) {
									
									simArr[i][j] = simArr[j][i] = 0.0;
								} else {
									
									String[] commonParentsId = MyArray.intersect(
											parentsIdi.split(" "), parentsIdj.split(" "));
									if (commonParentsId.length == 0) { // 没有公共的父节点
										
										simArr[i][j] = simArr[j][i] = 0.0;
										continue;
									}
									String maxCommonParentId = MyArray.maxValueInArray(commonParentsId);
									if(null == maxCommonParentId || maxCommonParentId.length() == 0 
											|| !categoryIdLevelHashMap.containsKey(new Integer(maxCommonParentId))) {
										
										continue;
									}
									int maxCommonParentIdLevel = categoryIdLevelHashMap.get(new Integer(maxCommonParentId));
									// 注意加1是因为层数从0开始，可以虚拟有-1层，即为根节点
									simArr[i][j] = simArr[j][i] = (maxCommonParentIdLevel + 1.0) / (level + 1.0); 
								}
							}
						}
					}
					double simDoubleArr[] = new double[itemsArr.length];
					double simTotal = 0.0; 
					for (int i = 0; i < itemsArr.length; i++) {
						
						double sim = 0;
						for (int j = 0; j < itemsArr.length; j++) {
							
							sim += simArr[i][j];
						}
						sim = sim / itemsArr.length;
						simDoubleArr[i] = sim;
						simTotal += sim;
					}
					for (int i = 0; i < itemsArr.length; i++) {
						
						simDoubleArr[i] = simDoubleArr[i] / simTotal;
					}
					String usersIdArr[] = usersIdSB.toString().split(" ");
					String itemsb = getItemsIdByCategoryId(itemsArr); //attention this item is category of rearranged
					int totalReview = getTotalReviewByUserIdItemId(usersIdArr,itemsb.trim().split(" "));

					dbo.insertIntoGroupTable(level, itemsb.substring(0, itemsb.length()/10).trim().toString(), support, usersIdSB.toString(), totalReview, itemsb.length());
					dbo.insertIntoGroupCategoryTable(groupId, itemsArr, simDoubleArr);
					dbo.insertIntoGroupUserTable(groupId, usersIdArr, itemsb.trim().split(" "), hm);
					groupId++;
					
				}
			}
			reader.close();
			fr.close();
			file = null;
			fr = null;
			System.out.println(this.getClass().getName() 
					+ " filter level " 
					+ level + "done , time cost is " 
					+ (System.currentTimeMillis() - begin) / 1000);
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	private String getItemsIdByCategoryId(String[] categoryIdArr) {

		StringBuilder itemsb = new StringBuilder();
		for(String cat : categoryIdArr) {
			
			if(null != hm.categoryItems.get(hm.ordercategory.get(Integer.valueOf(cat)))) {
				
				String itemsId = hm.categoryItems.get(hm.ordercategory.get(Integer.valueOf(cat)));
				if (itemsId != null) {
					
					itemsb.append(itemsId.trim() + " ");
				}
				
			}
		}
		return itemsb.toString();
	}

	private int getTotalReviewByUserIdItemId(String[] usersIdArr, String[] itemsIdArr) {
		
		if (itemsIdArr.length == 0 || usersIdArr.length == 0) {
			
			return 0;
		}
		
		int totalReview = 0;
		
		for(String uid : usersIdArr) {
			
			HashMap<Integer, Double> pidRating = hm.uidPidRatingHMInTrain.get(Integer.valueOf(uid));
			
			if(null != pidRating) {
				
				for(String pid : itemsIdArr) {
					
					if( !pid.equals("") && null != pidRating.get(Integer.valueOf(pid))){
						
						totalReview++;
					}
				}
			}
		}
		return totalReview;
	}
	
	private boolean haveUncoveredCategory(String itemsArr[],
			HashSet<Integer> leftCategory) {
		
		for (int i = 0; i < itemsArr.length; i++) {
			
			if (leftCategory.contains(new Integer(itemsArr[i]))) {
				
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 若该分类在下一层已经覆盖，则删除
	 * 
	 * @param filterSet
	 * @param level
	 * @return 返回剩下没有被覆盖的category
	 */
	private HashSet<Integer> filterCategory(HashSet<Integer> filterSet,
			HashMap<Integer, String> categoryHashMap) {
		
		HashSet<Integer> leftCategory = new HashSet<Integer>();
		Iterator<Integer> iter = categoryHashMap.keySet().iterator();
		String file_separator = System.getProperty("file.separator");
		File uncoveredFile = new File(Constants.PATH + "/data" + file_separator + "uncovered.dat");
		FileWriter fw;
		BufferedWriter bw;
		try {
			
			fw = new FileWriter(uncoveredFile);
			bw = new BufferedWriter(fw);
			while (iter.hasNext()) {
				
				Integer categoryId = iter.next();
				if (!filterSet.contains(categoryId)) {
					
					leftCategory.add(categoryId);
					bw.write(categoryId.intValue() + " ");
				}
			}
			bw.newLine();
			bw.flush();
			bw.close();
			fw.close();
			fw = null ; bw = null;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return leftCategory;
	}
	
	private int getCateLevel(int i) {
		
		if(i == 2) {
			
			return 100;
		}
		if(i == 1) {
			
			return 100;
		}
		return 4;
	}
}
