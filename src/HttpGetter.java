import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import javax.net.ssl.HttpsURLConnection;


public class HttpGetter {
	static public void main(String args[]){
		HttpGetter hg = HttpGetter.findIpInfo("159.226.191.159");
		//System.out.print("https://www.cnnic.cn");
		hg.getIpInfo();
	}
	
	private static String ip ;
	/*
	 * java_bean : ip 
	 * */
	public String getIp(){
		return this.ip;
	}
	
	private HttpGetter(String ip){
		this.ip = ip;
	}

	private HashMap<String, String> hm;
	/*
	 * java_bean : Report HashMap
	 * */
	public HashMap<String, String> getPropertyMap(){
		return hm;
	}
	
	
	/*
	 * 静态实例化方法（参数-ip）
	 * */
	public static HttpGetter findIpInfo(String ip){
		if(regexIp(ip)){
			HttpGetter hp =  new HttpGetter(ip);
			hp.getIpInfo();
			return hp;
		}
		else
			return null;
	}
	
	/*
	 * 判断IP地址是否合法
	 * */
	public static boolean regexIp(String ip){
		//remove white char 
		ip = ip.replaceAll("\\s", ""); 
		String[] ips = ip.split("\\.");
		if(ips.length==4){
			for(int i=0;i<4;i++){
				int intip = Integer.parseInt(ips[i]);
				if(intip<0||intip>255)
					return false;
			}
			return true;
		}else
			return false;
	}
	
	/*
	 *HTTP请求CNNIC_WHOIS_IPV4查询
	*/
	private void getIpInfo() {
		String urlStr = 
				"http://ipwhois.cnnic.cn/bns/query/Query/ipwhoisQuery.do?queryOption=ipv4&txtquery=";
		try{	
			urlStr += this.ip;
			URL url = new URL(urlStr);
			HttpURLConnection con = (HttpURLConnection)url.openConnection();
			con.setDoInput(true);
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent",
					"Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");
			//input stream
			InputStream is = con.getInputStream();
			//stream to streamReader
			InputStreamReader isReader = new InputStreamReader(is, "utf-8");
			//buffer reader 
			BufferedReader br = new BufferedReader(isReader);
			String str = br.readLine();
			StringBuilder sb = new StringBuilder();
			while(str!=null){
				sb.append(str);
				str = br.readLine();
			}	
			
			//html=>HashMap
			this.hm = matchTable(sb.toString());
			//System.out.println("get "+hm.size()+" report!");
			
			br.close();
			isReader.close();
			is.close();
			con.disconnect();
		}catch(IOException e){
			System.out.println(urlStr+" is error!");
			e.printStackTrace();
		}
		
		
	}
	/*
	 *使用正则表达式匹配网页表格内容  html=>HashMap
	*/
	private HashMap<String, String> matchTable(String s){
		HashMap<String, String> hm = new HashMap<>();
		//匹配表达式:<tr>.*?</tr>
		Pattern ptr = Pattern.compile("<tr>.*?</tr> ");
		Matcher mtr = ptr.matcher(s);
		
		//匹配表达式:  >.*?<	
		Pattern pfont = Pattern.compile(">.*?<");
		while(mtr.find()){
			//去空格
			String tmp = mtr.group().replaceAll("\\s{2,}", "");  //  \\s*
			tmp = tmp.replaceAll("&nbsp;", "");
			
			Matcher mmp = pfont.matcher(tmp);
			String[] kv = new String[2];
			int count = 0;
			while(mmp.find()){
				String ftmp = mmp.group();
				if(ftmp.length()>2){
					if(count<2)
						kv[count++] = ftmp.substring(1,ftmp.length()-1);
					else{
						count++;
						break;
					}
				}	
				//test
				//System.out.println(ftmp);
			}
			
			if(count==2){ //input to the map
				if(hm.containsKey(kv[0])){
					String frontV = hm.get(kv[0]);
					if(!frontV.equals(kv[1]))
						kv[1]= frontV+"||"+kv[1];
				}
				hm.put(kv[0], kv[1]);
			}	
			/*else
				System.out.println("error report front!");*/
		}
		
		return hm;
	}
	
}
