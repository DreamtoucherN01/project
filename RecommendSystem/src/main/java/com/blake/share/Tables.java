package com.blake.share;

public enum Tables {
	
	//epinions data
	review("review",1),
	product("product",2),
	category("category",3),
	
	useritemrating("useritemrating",4),
	useritemratingtest("useritemratingtest",12),
	categoryparent("categoryparent",5),
	itemcategory("itemcategory",6),
	levelcategory("levelcategory",7),
	grouptable("grouptable",8),
	group_category("group_category",9),
	group_user("group_user",10),
	recommendation("recommendation",11);
	
	String tablename;
	int tabletag;
	
	Tables(String tablename, int tabletag) {
		
		this.tablename = tablename;
		this.tabletag = tabletag;
	}
	
	public static Tables fromString(String tablename) {
		
		for(Tables table : Tables.values()) {
			
			if((table.toString()).equals(tablename)) {
				
				return table;
			}
		}
		return null;
	}
	
	public String getTableName() {
		
		return this.tablename;
	}
	
	public int getTableTag() {
		
		return this.tabletag;
	}

}
