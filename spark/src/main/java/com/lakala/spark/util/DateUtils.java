package com.lakala.spark.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * 日期时间工具类
 *
 */
public class DateUtils {
	
	public static final SimpleDateFormat TIME_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = 
			new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * 判断一个时间是否在另一个时间之前
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			if(dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 判断一个时间是否在另一个时间之后
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			if(dateTime1.after(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 计算时间差值（单位为秒）
	 * @param time1 时间1
	 * @param time2 时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(millisecond / 1000));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取年月日和小时
	 * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}  
	
	/**
	 * 获取当天日期（yyyy-MM-dd）
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		return DATE_FORMAT.format(new Date());
	}


	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());  
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		Date date = cal.getTime();
		
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期（yyyy-MM-dd）
	 * @param date Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}
	
	/**
	 * 解析时间字符串
	 * @param time 时间字符串
	 * @return
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Date parseDate(String time) {
		try {
			return DATE_FORMAT.parse(time);

		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static final String fmtYYYYMMDD = "yyyyMMdd";
	public static String getYMD() {
		String fmtNow = new SimpleDateFormat(fmtYYYYMMDD).format(Calendar.getInstance().getTime());
		return fmtNow;
	}

	public static String formatYMD(Date time) {
		String fmtNow = new SimpleDateFormat(fmtYYYYMMDD).format(time);
		return fmtNow;
	}

	public static Calendar startDate2015() {
		Calendar cal = new GregorianCalendar(2015, 0, 1,0,0,0);
		return cal;
	}

	public static Calendar addDate(Calendar in, int days) {
		Calendar date = in;
		date.add(Calendar.DAY_OF_MONTH, days);
		return date;
	}

	public static boolean compareSysdate(Calendar in) {
		Calendar date = in;
		Calendar sys = Calendar.getInstance();//使用默认时区和语言环境获得一个日历。

		if (date.getTime().after(sys.getTime())) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 *
	 * @return yyyy-[m]m-[d]d hh:mm:ss
	 */
	public static Timestamp getTime(Date date) {
		if(date == null){
			return new Timestamp((new Date()).getTime());
		} else {
			return new Timestamp(date.getTime());
		}

	}
	public static Timestamp getSysTimeStart() {
		return new Timestamp(parseDate(getTodayDate()).getTime());
	}
}
