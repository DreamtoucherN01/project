/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.blake.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
/**
 *
 * @author zhen
 */
public class MyArray {
    //两个集合的交集
    public static String[] intersect(String[] arr1, String[] arr2) {
        //这里会自动去重
        Map<String, Boolean> map = new HashMap<>();
        LinkedList<String> list = new LinkedList<>();
        for (String str : arr1) {
            if (!map.containsKey(str)) {
                map.put(str, Boolean.FALSE);
            }
        }
        for (String str : arr2) {
            if (map.containsKey(str)) {
                map.put(str, Boolean.TRUE);
            }
        }
        for (Map.Entry<String, Boolean> e : map.entrySet()) {
            if (e.getValue().equals(Boolean.TRUE)) {
                list.add(e.getKey());
            }
        }
        String[] result = {};
        return list.toArray(result);
    }
    

	public static Integer[] intersect(Integer[] arr1, Integer[] arr2) {

		arr1=MyArray.unique(arr1);
        arr2=MyArray.unique(arr2);
        Integer[] result={};
        boolean flag;
        for(int i=0;i<arr1.length;i++)
        {
            flag=false;
            for(int j=0;j<arr2.length;j++)
            {
                if(arr1[i]<arr2[j]){
                    break;
                }
                if((arr1[i]).equals(arr2[j]))
                {
                    flag=true;
                    break;
                }
            }
            if(flag)
            {
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=arr1[i];
            }
        }
        return result;
	}

	//两个集合的交集
    public static int[] intersect(int[] arr1, int[] arr2){
        arr1=MyArray.unique(arr1);
        arr2=MyArray.unique(arr2);
        int result[]={};
        boolean flag;
        for(int i=0;i<arr1.length;i++)
        {
            flag=false;
            for(int j=0;j<arr2.length;j++)
            {
                if(arr1[i]<arr2[j]){
                    break;
                }
                if(arr1[i]==arr2[j])
                {
                    flag=true;
                    break;
                }
            }
            if(flag)
            {
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=arr1[i];
            }
        }
        return result;
    }
    
    //两个集合的并集
    public static String[] union(String[] arr1,String[] arr2){
        Set<String> s=new HashSet<>();
        s.addAll(Arrays.asList(arr1));
        s.addAll(Arrays.asList(arr2));
        String[] result = s.toArray(new String[0]);
        return result;
    }
    

	public static Integer[] union(Integer[] arr1, Integer[] arr2) {

		Set<Integer> s=new HashSet<>();
        s.addAll(Arrays.asList(arr1));
        s.addAll(Arrays.asList(arr2));
        Integer[] result = s.toArray(new Integer[0]);
        return result;
	}
    
    //两个集合的并集
    public static int[] union(int[] arr1,int[] arr2){
        arr1=MyArray.unique(arr1);
        arr2=MyArray.unique(arr2);
        int result[]=Arrays.copyOf(arr1, arr1.length);
        boolean flag;
        for(int i=0;i<arr2.length;i++){
            flag=false;
            for(int j=0;j<arr1.length;j++)
            {
                if(arr2[i]<arr1[j]){
                    break;
                }
                if(arr2[i]==arr1[j])
                {
                    flag=true;
                    break;
                }
            }
            if(!flag){
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=arr2[i];
            }
        }
        return result;
    }
    
    //集合元素去重
    public static int[] unique(int[] arr){
        if(arr.length==0){
            int[] result={};
            return result;
        }
        int[] temp=Arrays.copyOf(arr, arr.length);
        Arrays.sort(temp);
        int[] result={temp[0]};
        for(int i=1;i<temp.length;i++){
            if(temp[i]!=temp[i-1])
            {
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=temp[i];
            }
        }
        return result;
    }
    
    private static Integer[] unique(Integer[] arr) {

    	if(arr.length==0){
            Integer[] result={};
            return result;
        }
        Integer[] temp=Arrays.copyOf(arr, arr.length);
        Arrays.sort(temp);
        Integer[] result={temp[0]};
        for(int i=1;i<temp.length;i++){
            if(temp[i]!=temp[i-1])
            {
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=temp[i];
            }
        }
        return result;
	}
    
    //集合元素去重
    public static String[] unique(String[] arr){
        if(arr.length==0){
            String[] result={};
            return result;
        }
        String[] temp=Arrays.copyOf(arr, arr.length);
        Arrays.sort(temp);
        String[] result={temp[0]};
        for(int i=1;i<temp.length;i++){
            if(!temp[i].equals(temp[i-1]))
            {
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=temp[i];
            }
        }
        return result;
    }
    
    //判断son集合是否包含在father集合里面
    public static boolean isContain(String[] son,String[] father){
        Arrays.sort(father);
        for(int i=0;i<son.length;i++)
        {
            boolean flag=false;
            for(int j=0;j<father.length;j++)
            {
                if(son[i].compareTo(father[j])<0){
                    break;
                }
                if(son[i].equals(father[j]))
                {
                    flag=true;
                    break;
                }
            }
            if(!flag)
            {
                return false;
            }
        }
        return true;
    }
    
    //判断son集合是否包含在father集合里面
    public static boolean isContain(int[] son,int[] father){
        Arrays.sort(father);
        for(int i=0;i<son.length;i++)
        {
            boolean flag=false;
            for(int j=0;j<father.length;j++)
            {
                if(son[i]<father[j]){
                    break;
                }
                if(son[i]==father[j])
                {
                    flag=true;
                    break;
                }
            }
            if(!flag)
            {
                return false;
            }
        }
        return true;
    }
    
    //计算两个集合的差集
    public static int[] difference(int[] arr1,int[] arr2){
        arr1=MyArray.unique(arr1);
        arr2=MyArray.unique(arr2);
        int result[]={};
        boolean flag;
        for(int i=0;i<arr1.length;i++)
        {
            flag=false;
            for(int j=0;j<arr2.length;j++)
            {
                if(arr1[i]<arr2[j]){
                    break;
                }
                if(arr1[i]==arr2[j])
                {
                    flag=true;
                    break;
                }
            }
            if(!flag)
            {
                result=Arrays.copyOf(result, result.length+1);
                result[result.length-1]=arr1[i];
            }
        }
        return result;
    }
    
    //计算两个集合的差集
    public static String[] difference(String[] arr1, String[] arr2) {
        //这里会自动去重
        Map<String, Boolean> map = new HashMap<>();
        LinkedList<String> list = new LinkedList<>();
        for (String str : arr1) {
            if (!map.containsKey(str)) {
                map.put(str, Boolean.FALSE);
            }
        }
        for (String str : arr2) {
            if (map.containsKey(str)) {
                map.put(str, Boolean.TRUE);
            }
        }
        for (Map.Entry<String, Boolean> e : map.entrySet()) {
            if (e.getValue().equals(Boolean.FALSE)) {
                list.add(e.getKey());
            }
        }
        String[] result = {};
        return list.toArray(result);
    }
    
    public static String arrayToString(String str[],String delimiter){
        StringBuilder sb=new StringBuilder("");
        for(int i=0;i<str.length;i++){
            sb.append(str[i]).append(delimiter);
        }
        return sb.toString();
    }
    
    public static String arrayToString(int num[],String delimiter){
        StringBuilder sb=new StringBuilder("");
        for(int i=0;i<num.length;i++){
            sb.append(num[i]).append(delimiter);
        }
        return sb.toString();
    }
    
    public static int[] stringArrayToIntArray(String str[]){
        int[] num=new int[str.length];
        for(int i=0;i<str.length;i++){
            try{
                num[i]=Integer.parseInt(str[i]);
            }catch(Exception e){
            }
        }
        return num;
    }
    
    public static String[] intArrayToStringArray(int num[]){
        String[] str=new String[num.length];
        for(int i=0;i<num.length;i++){
            str[i]=num[i]+"";
        }
        return str;
    }
    
    public static int maxValueInArray(int num[]){
        if(num.length==0){
            return Integer.MIN_VALUE;
        }
        int max=num[0];
        for(int i=1;i<num.length;i++){
            if(max<num[i]){
                max=num[i];
            }
        }
        return max;
    }
    
    public static int minValueInArray(int num[]){
        if(num.length==0){
            return Integer.MIN_VALUE;
        }
        int min=num[0];
        for(int i=1;i<num.length;i++){
            if(min>num[i]){
                min=num[i];
            }
        }
        return min;
    }
    
    //只适合于正整数
    public static String maxValueInArray(String str[]){
        if(str.length==0){
            return Integer.MIN_VALUE+"";
        }
        String max=str[0];
        for(int i=1;i<str.length;i++){
            if(max.compareTo(str[i])<0){
                max=str[i];
            }
        }
        return max;
    }
    
    public static String minValueInArray(String str[]){
        if(str.length==0){
            return Integer.MIN_VALUE+"";
        }
        String min=str[0];
        for(int i=1;i<str.length;i++){
            if(min.compareTo(str[i])>0){
                min=str[i];
            }
        }
        return min;
    }

    public static int getIndexInArray(int num, int[] numArr){
        for(int i=0;i<numArr.length;i++){
            if(numArr[i]==num){
                return i;
            }
        }
        return -1;
    }

    public static int getIndexInArray(Integer num, Integer[] numArr){
        for(int i=0;i<numArr.length;i++){
            if(numArr[i].intValue()==num.intValue()){
                return i;
            }
        }
        return -1;
    }
    
    public static int getIndexInArray(String str, String[] strArr){
        for(int i=0;i<strArr.length;i++){
            if(str.equals(strArr[i])){
                return i;
            }
        }
        return -1;
    }
}