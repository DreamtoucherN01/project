/*
 * 类功能：连接数据库
 * 作者：李瑞远
 */
package com.blake.database;

import java.io.*;
import java.sql.*;
import java.util.*;
/**
 *
 * @author leo
 */
public class DBConnection {
	
    private String driver=null;		//驱动程序
    private String url=null;		//odbc数据源
    private String urlmib=null;		//odbc数据源
    private String username=null;	//用户名
    private String password=null;	//密码
    private Connection conSrc=null;
    private Connection conWorkspace=null;

    public DBConnection(){
        Properties p=new Properties();
        try{
        	
            System.getProperty("file.separator");
            BufferedInputStream in= (BufferedInputStream) this.getClass()
            		.getClassLoader().getResourceAsStream("./db.properties");
            p.load(in);
            driver = p.getProperty("jdbc.driver");
            url = p.getProperty("jdbc.url");
            urlmib = p.getProperty("jdbc.urlmib");
            username = p.getProperty("username");
            password = p.getProperty("password");
            in.close();
        }catch(FileNotFoundException ex){
        	
            ex.printStackTrace();
        }catch(IOException ex){
        	
            ex.printStackTrace();
        }
    }

    public DBConnection(String driver,String url,String username,String password){
    	
        this.driver=driver;
        this.url=url;
        this.username=username;
        this.password=password;
    }
    //创建数据库连接
    public Connection makeSourceConnection(){
    	conSrc=null;
        try{
        	
            Class.forName(driver);
            conSrc=DriverManager.getConnection(url,username,password);
        }catch(SQLException sqle){
        	
            sqle.printStackTrace();
        }catch(ClassNotFoundException ex){
        	
            ex.printStackTrace();
        }
        return conSrc;
    }
    
    public Connection makeWorkspaceConnection(){
    	
    	conWorkspace=null;
        try{
        	
            Class.forName(driver);
            conWorkspace=DriverManager.getConnection(urlmib,username,password);
        }catch(SQLException sqle){
        	
            sqle.printStackTrace();
        }catch(ClassNotFoundException ex){
        	
            ex.printStackTrace();
        }
        return conWorkspace;
    }
    //关闭数据库连接
    public void closeConnection(){
    	
        try{
        	
        	conSrc.close();
        	conWorkspace.close();
        }catch(SQLException ex){
        	
            ex.printStackTrace();
        }
    }
}