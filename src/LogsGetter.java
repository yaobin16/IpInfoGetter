import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LogsGetter {
	
	class Log{
		// ip address
		public String ip;
		// request time
		public String requestTime;
		// request info
		public String requestType;
		// request URL
		public String requestURL;
		
		public boolean complete(){
			if(requestURL==null||ip==null)
				return false;
			else {
				// time default => 1980-1-1
				if(requestTime==null)
					requestTime = "1980-1-1";
				// request Type default => GET
				if(requestType==null)
					requestType = "GET"; 
				return true;
				
			}
		}
	}
	
	
	private String path ;
	private List<Log> logs;
	public static void main(String[] args) {
		LogsGetter lg = new LogsGetter("logs/english.cas.cn-20180325-access_log");
		lg.upToData();
				
				/*if(!hm.containsKey(now.ip)&&count<50){
					HttpGetter hGetter = HttpGetter.findIpInfo(now.ip);
					String name = hGetter.getPropertyMap().get("单位描述:");
					if(name!=null)
						System.out.println(count+":	ip"+now.ip+" 单位描述:"+name);
					count++;
				}
				hm.put(now.ip, now);*/
			
		
		//System.out.println("logs:"+lg.logs.size());
		
	}
	
	/*
	 * 构造方法 LogsGetter
	 * 参数：[logsPath] =>日志文件路径
	 * */
	public LogsGetter(String logsPath){
		this.path = logsPath;
		System.out.println("reading...");
		this.logs = patternLogsFile();
		System.out.println("读取日志记录"+this.logs.size()+"条");
	}
	
	/*
	 * 正则匹配日志记录
	 * */
	private List<Log> patternLogsFile(){
		
		
		try{
			File fl = new File(this.path);
			Scanner sc = new Scanner(fl);
			List<Log> res = new ArrayList<>();
			//匹配正则式
			Pattern p = Pattern.compile
					("(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3})"
							+"(\\s)(\\S+|-)(\\s)(\\S+|-)"
							+"(\\s\\[)([0-9a-zA-Z\\+-:/ ]+)"
							+"(\\]\\s\\\")(\\S+)(\\s)(\\S+)(\\s)(\\S+)(\\\"\\s)(\\d+)(\\s)(\\d+)(\\s\\\")(\\S+)(\\\"\\s\\\")(.+)(\\\")");
			Matcher m = null;	
			while(sc.hasNext()){
				m = p.matcher(sc.nextLine());
				if(m.find()){
					Log now = resolveLog(m.group());
					if(now!=null)
						res.add(now);
				}
			}		
			sc.close();
			return res;
			
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return null;
		}
		
	}
	
	/*
	 * 解析日志信息 String => Log
	 * */
	public Log resolveLog(String log){
		
		Log l = new Log();
		//check has request success!
		Pattern pSuccess = Pattern.compile("\\s200\\s");
		
		if(pSuccess.matcher(log).find()){
			Pattern p_ip = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
			Matcher m_ip = p_ip.matcher(log);
			if(m_ip.find())
				l.ip = m_ip.group();
					
			Pattern p_time = Pattern.compile("(\\[[^\\[\\]]+\\])\\s");
			Matcher m_time = p_time.matcher(log);
			if(m_time.find()){
				String fullTime =  m_time.group();
				l.requestTime = fullTime.substring(1,fullTime.length()-2);
			}
				
			
			
			Pattern p_request = Pattern.compile("\"((?:[^\\\"]|\\\")+)\\.s?html\\s");
			Matcher m_requset = p_request.matcher(log);
			String fullRequet =null ;
			if(m_requset.find())
				fullRequet = m_requset.group();
			if(fullRequet!=null){
				String[] rUrls =fullRequet.split(" ");
				if(rUrls.length>1){
					l.requestType = rUrls[0].substring(1,rUrls[0].length());
					l.requestURL = rUrls[1];
				}
			}	
				
		}
		
		if(l.complete())
			return l;
		else 
			return null;
		
		
	}
	
	/*
	 * 日志访问记录写入数据库
	 * */
	public int upToData(){

		int success = 0;
		DataConnect dc = null;
		System.out.println("writting...");
				
		int count = 0;
		int maxinput = 500; //max input line
		Iterator<Log>  it = this.logs.iterator();	
		while(it.hasNext()){
			if(count%maxinput==0)  //if large than max input line, create a new date connect
				dc = new DataConnect();
			Log now = it.next();
			//find website && site?
			success += dc.insertLogs(now.ip, now.requestTime, now.requestURL,now.requestType,5,417); 	
			count ++;
		}

		System.out.println("成功插入数据库"+success+"条");
		return success;
	}
	
	
	/*
	 * IP地址填充(10.0.1.100 => 010.000.001.100)
	 * */
	public static String ToFullIp(String ip){
		String ips[] = ip.split("\\.");
		if(ips.length!=4)
			return null;
		StringBuilder ipBuilder = new StringBuilder();
		for(int i=0;i<4;i++){
			if(ips[i].length()<3){
				for(int j=0;j<3-ips[i].length();j++)
					ipBuilder.append("0");
			}
			ipBuilder.append(ips[i]);
			ipBuilder.append(".");
		}
		return ipBuilder.deleteCharAt(ipBuilder.length()-1).toString();
	}
	
}
