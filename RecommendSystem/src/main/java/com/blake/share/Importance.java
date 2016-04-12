package com.blake.share;

public enum Importance {

	Helpful("Helpful",2),
	Somewhat("Somewhat",3),
	Very("Very",4),
	Not("Not",0),
	Show("Show",1);
	
	String importance;
	int level;
	
	Importance(String importance, int level) {
		
		this.importance = importance;
		this.level = level;
	}
	
	public static Importance fromString(String importance) {
		
		for(Importance imp : Importance.values()) {
			
			if((imp.toString()).equals(importance)) {
				
				return imp;
			}
		}
		return null;
	}
	
	public String getImportance() {
		
		return this.importance;
	}
	
	public int getLevel() {
		
		return this.level;
	}
}
