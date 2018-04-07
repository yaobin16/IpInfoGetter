import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class DataConnect {
	
	private static String USERNAME = "dbusername";
	private static String PASSWROD = "dbpassword";
	private static String DRVIER = "oracle.jdbc.OracleDriver"; 
	private static String URL = "jdbc:oracle:thin:@db_url:dbname";   

	//create connection 
	Connection connection = null;
	//prestate 
	PreparedStatement ps = null;
	//result
	ResultSet rs = null;
	
	public static void main(String[] args){
		DataConnect dc = new DataConnect();
		List<String> full = dc.getNullInfoLogs();
		System.out.println(full.size()+"unique ip!");
		int total = 0;
		for(int i=0;i<full.size();i++){
			total+=dc.updateIpInfo(full.get(i));
			if(total%100==0)
				System.out.println(total+"report update");
		}
			
		System.out.println("Result:"+total+"changed!");
	}
	
	public DataConnect(){
		getConnect();
	}
	
	/*
	 * 写入日志数据
	 * */
	public int insertLogs(String ip,String timestr,String url,String type, int platId,int siteId)  {	
		try{
			
			String countsql = "select count(*) from VISITINFO where 1=1";
			ps = connection.prepareStatement(countsql);
			rs = ps.executeQuery();
			int vid = 0;
			if(rs.next())
				vid = rs.getInt(1)+10000;
			
			String sql = "insert into VISITINFO(VID,ip,vtime,vurl,vtype,platid,siteid) values(?,?,?,?,?,?,?)";
			ps = connection.prepareStatement(sql);
			//insert 
			ps.setInt(1, vid);
			ps.setString(2, ip);
			
			//date transform
			Date dt = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.US);
			dt = sdf.parse(timestr);
			Timestamp ts = new Timestamp(dt.getTime());
			ps.setTimestamp(3, ts);
			
			ps.setString(4, url);
			ps.setString(5, type);
			
			ps.setInt(6, platId);
			ps.setInt(7, siteId);
			
			int count = ps.executeUpdate();
			ps.close();
			return count;
			
		}catch(Exception e){
			System.out.print("error!");
			e.printStackTrace();
		}
		
		
		return 0;
	}
	
	public List<String> getNullInfoLogs(){
		
		List<String> ip_ls = new ArrayList<>();
		try{
			String queryStr = "select ip from visitinfo where pid is null group by ip"; //unique ip 
			ps = connection.prepareStatement(queryStr);
			rs = ps.executeQuery();
			while(rs.next())
				ip_ls.add(rs.getString(1));
			ps.close();
		}catch(SQLException e){
			System.out.println("Load logs error!");
		}
		
		return ip_ls;
	}
	
	/*
	 * 更新IP信息（根据IP地址查询IPINFO表，如无信息则调用HttGetter查询WHOIS并写入信息）
	 *@IP:更新IP地址
	 *@返回值:修改数据条数
	 * */
	public int updateIpInfo(String ip){
		
		try{
			int pid = 0;
			//search the whois and find the ip info 
			HttpGetter hg = HttpGetter.findIpInfo(ip);
			HashMap<String, String> queryResMp = hg.getPropertyMap();
				
			String qureryStr = "select pid from ipinfo where ipbegin =? and  ipend =? ";
			ps = connection.prepareStatement(qureryStr);
			
			//trans into full ip (query ipinfo table!!)
			String ip_origin = queryResMp.get("IPv4地址段:");
			String[] ips = ip_origin.replaceAll(" ", "").split("-");
			if(ips.length>1){
				ps.setString(1, LogsGetter.ToFullIp(ips[0])); //trans to full ip
				ps.setString(2, LogsGetter.ToFullIp(ips[1])); 
			}else{
				ps.setString(1, "0.0.0.0");
				ps.setString(2, "0.0.0.0");
			}
			

			rs = ps.executeQuery();
			
			if(rs.next())
				pid = rs.getInt(1);
			else{
				pid = insertIpInfo(ip,queryResMp);
			}
			//update the visitinfo.pid
			String updateStr = "update visitinfo set pid = ? where ip = ?";
			ps = connection.prepareStatement(updateStr);
			ps.setInt(1, pid);
			ps.setString(2, ip);
			int rowcount = ps.executeUpdate();
			ps.close();
			return rowcount;
			
		}catch(SQLException e){
			System.out.println("update IpInfo error!");
		}
	
		return 0;
	}
	
	/*
	 * 查询ip信息并写入
	 * 返回值：VIP（ipinfo主键）
	 * */
	private int insertIpInfo(String ip, HashMap<String, String> queryResMp){
		int pid = -1;
		try{
			String countsql = "select count(*) from ipinfo where 1=1";
			ps = connection.prepareStatement(countsql);
			rs = ps.executeQuery();
			if(rs.next())
				pid = rs.getInt(1)+10000;
			
			String updateStr = "insert into ipinfo(pid,ipbegin,ipend,netname,unitdesc,statecode,contact,address,tel,fax,email,source,ipstatus) " 
					+"values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			ps = connection.prepareStatement(updateStr);
			ps.setInt(1, pid); //1-pid
			
			//2-ipbegin 3-ipend
			String ip_origin = queryResMp.get("IPv4地址段:");
			String[] ips = ip_origin.replaceAll(" ", "").split("-");
			if(ips.length>1){
				ps.setString(2, LogsGetter.ToFullIp(ips[0])); //trans to full ip
				ps.setString(3, LogsGetter.ToFullIp(ips[1])); 
			}else{
				ps.setString(2, "0.0.0.0");
				ps.setString(3, "0.0.0.0");
			}
			
			//4-netname
			String netname = queryResMp.get("网络名称:");
			if(netname!=null)
				ps.setString(4, netname);
			else {
				ps.setString(4, "-");
			}
			
			//5-unitdesc
			String unitdesc = queryResMp.get("单位描述:");
			if(unitdesc!=null)
				ps.setString(5, unitdesc);
			else {
				ps.setString(5, "-");
			}
			
			//6-statecode
			String statecode = queryResMp.get("国家代码:");
			if(statecode!=null)
				ps.setString(6, statecode);
			else 
				ps.setString(6, "-");
			
			
			//7-contact
			String contact = queryResMp.get("姓名:");
			if(contact!=null)
				ps.setString(7, contact);
			else 
				ps.setString(7, "-");
			
			
			//8-address
			String address = queryResMp.get("通讯地址:");
			if(address!=null)
				ps.setString(8, address);
			else 
				ps.setString(8, "-");
			
			//9-tel
			String tel = queryResMp.get("办公电话:");
			if(tel!=null)
				ps.setString(9, tel);
			else 
				ps.setString(9, "-");
			
			//10-fax
			String fax = queryResMp.get("传真:");
			if(fax!=null)
				ps.setString(10, fax);
			else 
				ps.setString(10, "-");
			

			//11-email
			String email = queryResMp.get("邮件地址:");
			if(email!=null)
				ps.setString(11, email);
			else 
				ps.setString(11, "-");
			
			//12-source
			String source = queryResMp.get("数据来源:");
			if(source!=null)
				ps.setString(12, source);
			else 
				ps.setString(12, "-");
			
			//13-source
			String ipstatus = queryResMp.get("地址状态:");
			if(ipstatus!=null)
				ps.setString(13,ipstatus);
			else 
				ps.setString(13, "-");
			
			if(ps.executeUpdate()==0)
				pid = -1;
			ps.close();
			
		}catch(SQLException e){
			System.out.println("insert IpInfo error! ");
			e.printStackTrace();
		}
		
		return pid;
	}
	
	private void getConnect() {
		try{
			Class.forName(DRVIER);
			connection = DriverManager.getConnection(URL,USERNAME,PASSWROD);
			//System.out.print("connect success!");
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
