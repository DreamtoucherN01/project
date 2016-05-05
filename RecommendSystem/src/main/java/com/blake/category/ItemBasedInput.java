package com.blake.category;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.blake.share.HashMapHarness;
import com.blake.share.Tables;
import com.blake.util.Constants;

public class ItemBasedInput {
	
	protected Connection conWorkspace;
	protected HashMapHarness hm;

	public ItemBasedInput(HashMapHarness hm, Connection conWorkspace) {

		this.hm = hm;
		this.conWorkspace = conWorkspace;
	}

	public void getGroupMiningInput() {

		System.out.println(this.getClass().getName() + " getGroupMiningInput");
		long begin = System.currentTimeMillis();
		try {
			
			String file_separator = System.getProperty("file.separator");
			String dirName = Constants.PATH + "/data/gen/";
			File dirFile = new File(dirName);
			dirFile.mkdirs();
			File fileWithoutUser[] = new File[Constants.CATEGORY_LEVEL_NUM];
			File fileWithUser[] = new File[Constants.CATEGORY_LEVEL_NUM];
			FileWriter fwWithoutUser[] = new FileWriter[Constants.CATEGORY_LEVEL_NUM];
			FileWriter fwWithUser[] = new FileWriter[Constants.CATEGORY_LEVEL_NUM];
			BufferedWriter bwWithoutUser[] = new BufferedWriter[Constants.CATEGORY_LEVEL_NUM];
			BufferedWriter bwWithUser[] = new BufferedWriter[Constants.CATEGORY_LEVEL_NUM];
			for (int i = 0; i < Constants.CATEGORY_LEVEL_NUM; i++) {
				
				fileWithoutUser[i] = new File(dirName + file_separator + "groupInputWithoutUser_" + i + ".dat");
				fileWithUser[i] = new File(dirName + file_separator + "groupInputWithUser_" + i + ".dat");
				fileWithoutUser[i].createNewFile();
				fileWithUser[i].createNewFile();

				fwWithoutUser[i] = new FileWriter(fileWithoutUser[i]);
				fwWithUser[i] = new FileWriter(fileWithUser[i]);

				bwWithoutUser[i] = new BufferedWriter(fwWithoutUser[i]);
				bwWithUser[i] = new BufferedWriter(fwWithUser[i]);
			}

			int[] categoryNumber = new int[Constants.CATEGORY_LEVEL_NUM];
			int totalCategoryNumber = 0;
			try {
				
				String sql = "select level,count(*) from " + Tables.levelcategory.getTableName() + " group by level order by level";
				PreparedStatement pre = conWorkspace.prepareCall(sql);
				ResultSet rs = pre.executeQuery();
				while (rs.next()) {
					
					categoryNumber[rs.getInt(1)] = rs.getInt(2);
					totalCategoryNumber += categoryNumber[rs.getInt(1)];
				}
				rs.close();
				pre.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			System.out.println("totalCategoryNumber : "+totalCategoryNumber);
			boolean[] reviewFlag = new boolean[totalCategoryNumber + 1];
				
			String sql="SELECT user,item FROM mib.useritemrating";
            PreparedStatement pre=conWorkspace.prepareCall(sql);
            ResultSet rs=pre.executeQuery();
			int lastUser = -1;
            while(rs.next()){
            	
            	int user=rs.getInt(1);
            	int item = rs.getInt(2);
                String categoryId = hm.itemCategoryStr.get(item);
				if (categoryId == null) {
					
					continue;
				} else {
					
					categoryId = categoryId.trim();
				}
				if (user != lastUser) {
					
					if (lastUser != -1) {
						
						for (int i1 = 0; i1 < Constants.CATEGORY_LEVEL_NUM; i1++) {
							
							int offset = 0;
							for (int j = 0; j < i1; j++) {
								
								offset += categoryNumber[j];
							}
							StringBuilder userData = new StringBuilder("");
							int total = sumOfCate(categoryNumber, i1);
							for (int j = offset; j <= total; j++) { 
																	
								if (reviewFlag[j] == true) {
									
									userData.append(j).append(" ");
								}
							}
							String userDataStr = userData.toString();
							if (!userDataStr.equals("")) {
								
								bwWithoutUser[i1].write(userDataStr);
								bwWithoutUser[i1].newLine();
								bwWithUser[i1].write(lastUser + " " + userDataStr);
								bwWithUser[i1].newLine();
							}
						}
					}
					for (int i1 = 1; i1 <= totalCategoryNumber; i1++) {
						
						reviewFlag[i1] = false;
					}
					lastUser = user;
				}
				String categoryIdArr[] = categoryId.trim().split(" ");
				for (int i1 = 0; i1 < categoryIdArr.length; i1++) {
					
					if (categoryIdArr[i1].equals("")) {
						
						continue;
					}
					reviewFlag[Integer.parseInt(categoryIdArr[i1])] = true;
				}
		    }

			for (int i = 0; i < Constants.CATEGORY_LEVEL_NUM; i++) {
				bwWithoutUser[i].flush();
				bwWithoutUser[i].close();
				fwWithoutUser[i].close();
				bwWithoutUser[i] = null ; fwWithoutUser[i] = null;
				
				bwWithUser[i].flush();
				bwWithUser[i].close();
				fwWithUser[i].close();
				bwWithUser[i] = null ; fwWithUser[i] = null;
			}
			
			reviewFlag = null;
			categoryNumber = null;
			System.out.println("getGroupMiningInput done , time cost is " + (System.currentTimeMillis() - begin)/1000 );
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	private int sumOfCate(int[] categoryNumber, int i) {
		int sum = 0;
		for (int j = 0; j <= i; j++) {
			
			sum += categoryNumber[j];
		}
		return sum;
	}
}
