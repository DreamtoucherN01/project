/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blake.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 * @author Talent
 */
public class Functions {
    /**
     * 功能：调用exe程序
     * @param command 命令行
     * @param wait 是否等待子进程运行完毕
     */
    public static void openExe(String command,boolean wait){
    	
    	
    	System.out.println(command);
        Runtime rn = Runtime.getRuntime();
        Process p;
        try {
            p = rn.exec(command);
          //获取进程的标准输入流  
            final InputStream is1 = p.getInputStream();   
            //获取进城的错误流  
            final InputStream is2 = p.getErrorStream();  
            new Thread() {  
                public void run() {  
                   BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));  
                    try {  
                        String line1 = null;  
                        while ((line1 = br1.readLine()) != null) {  
                              if (line1 != null){}  
                          }  
                    } catch (IOException e) {  
                         e.printStackTrace();  
                    }  
                    finally{  
                         try {  
                           is1.close();  
                         } catch (IOException e) {  
                            e.printStackTrace();  
                        }  
                      }  
                    }  
                 }.start(); 
            
            new Thread() {   
                public void  run() {   
                 BufferedReader br2 = new  BufferedReader(new  InputStreamReader(is2));   
                    try {   
                       String line2 = null ;   
                       while ((line2 = br2.readLine()) !=  null ) {   
                            if (line2 != null){}  
                       }   
                     } catch (IOException e) {   
                           e.printStackTrace();  
                     }   
                    finally{  
                       try {  
                           is2.close();  
                       } catch (IOException e) {  
                           e.printStackTrace();  
                       }  
                     }  
                  }   
                }.start();
            if(wait){
                p.waitFor();
                p.destroy();
            }
        } catch (Exception e) {
            System.out.println("Error exec! ["+command+"]");
        }
    }
}
