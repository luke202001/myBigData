package com.myBigData.MapReduce.weblogwash;

import java.util.Date;
import java.util.Locale;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;

public class WebLogParser {
	
	static SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static public WebLogBean parser(String line) {
		WebLogBean bean = new WebLogBean();
		String[] arr = line.split(" "); 
		
		if (arr.length > 11) {
			bean.setRemote_addr(arr[0]);
			bean.setRemote_user(arr[1]); 
			bean.setTime_local(parseTime(arr[3].substring(1)));	//  18/Sep/2013:06:49:36
			bean.setRequest(arr[6]);
			bean.setStatus(arr[8]);
			bean.setBody_bytes_sent(arr[9]);
			bean.setHttp_referer(arr[10]);
			
			//如果useragent元素较多，拼接useragent
			if (arr.length > 12) {
				StringBuilder sb = new StringBuilder();
				for(int i = 11; i < arr.length; i++){
					sb.append(arr[i]);
				}
				bean.setHttp_user_agent(sb.toString());
			} else {
				bean.setHttp_user_agent(arr[11]);
			}
			
			if (Integer.parseInt(bean.getStatus()) >= 400) {
				bean.setValid(false);
			}
			
			if("-invalid_time-".equals(bean.getTime_local())){
				bean.setValid(false);
			}
			
		} else {
			bean.setValid(false);
		}
		
		return bean;
	}
	
	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		if (!pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}
	
	static public String parseTime(String dt) {
		String timeString = "";
		
		try {
			Date parse = sdf1.parse(dt);
			timeString = sdf2.format(parse);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return timeString;
	}
}
