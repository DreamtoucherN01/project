/*
 * 此类定义了整个程序可能会用到的常量
 */
package com.blake.util;

/**
 *
 * @author Leo
 */
public class Constants {
	/**
	 * 用于实验的用户的个数
	 */
	public static int USER_NUMBER = 10000;

	/**
	 * 分类层次数目
	 */
	public static int CATEGORY_LEVEL_NUM = 6;

	/**
	 * 训练集占全部数据的比例
	 */
	public static double TRAIN_PROPORTION_IN_ALL = 0.6;

	/**
	 * 最顶层的分类中，最少的物品数
	 */
	public static int MIN_NUMBER_IN_TOP_LEVEL = 10;

	/**
	 * 最底层的最小支持度
	 */
	public static double BUTTOM_MIN_SUP = 0.63323;

	/**
	 * 合并的相似度阈值
	 */
	public static double DELTA = 10;

	/**
	 * 最小支持度损失率
	 */
	public static double MIN_SUP_LOSS_RATE = 3;

	/**
	 * 推荐时实验次数
	 */
	public static int RECOMMENDATION_TIMES = 1000;

	/**
	 * double类型的最大精度
	 */
	public static double DOUBLE_PRECISION = 0.000001;

	/**
	 * importance中，support/totalSupport所占比例
	 */
	public static double UR_PROPORTION = 0.5;

	/**
	 * 当用我们的方法无法预测后，是否用传统的cf方法继续
	 */
	public static boolean USE_CF_WHEN_FAILED = true;

	/**
	 * topN推荐
	 */
	public static int TOP_N_RECOMMENDATION = 20;

	/**
	 * 是否为user_base方法推荐
	 */
	public static boolean IS_USER_BASE = false;
	
	/**
	 * 路径
	 */
	public static String PATH = "D:\\workspace\\java\\recommend-engine\\Zibra";

	
}
