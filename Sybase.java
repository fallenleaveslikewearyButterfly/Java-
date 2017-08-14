import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Sybase {

	public static void main(String[] args)  {
		String sitename=args[0];
		String begindate=args[1];
		String swapdate=args[2];
		Connection conn=getconn();
		
		HashMap<String,String> cmdinfo=listfilename();
		Iterator iter = cmdinfo.entrySet().iterator();
		String sqlName=null;
		String sql=null;
		ResultSet rs=null;
		//�������
		ResultSetMetaData metaData=null;
		PrintWriter pw=null;
		PrintWriter pwlog=null;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//�������ڸ�ʽ
		
		while (iter.hasNext()) {   
			Map.Entry entry = (Map.Entry) iter.next();   
			sqlName=entry.getKey().toString();
			sqlName=sqlName.replace("C:\\Users\\enqrrwi\\Desktop\\Puerto Rico\\Puerto Rico\\07 SqlCommand\\", "");
			sqlName=sqlName.replace(".sql", "");
			sql=entry.getValue().toString();
			sql=sql.replace("NE349", sitename);
			sql=sql.replace("2017-05-04", begindate);
			sql=sql.replace("2017-05-31", swapdate);
			//sql=sql.replaceAll("'[A-Z]{2}\\d{3}'","'"+sitename+"'");
			rs=execsql(conn,sql);
			pw=output(sqlName,sitename);
			pwlog=output("log",sitename);
			if(rs==null){
				System.out.println(df.format(new Date())+" û�л�ȡ�������,"+sqlName);
				pwlog.write(df.format(new Date())+" û�л�ȡ�������,"+sqlName+"\n");
				pwlog.flush();
				rs=execsql(conn,sql);
				continue;
			}else{
				System.out.print(df.format(new Date())+" �ѳɹ���ȡ���������"+sqlName+"\n");
				pwlog.write(df.format(new Date())+" �ѳɹ���ȡ���������"+sqlName+"\n");
				pwlog.flush();
			}
			//������Ĵ���
			try {
				metaData=rs.getMetaData();
				for(int i=1;i<=metaData.getColumnCount();i++){
					pw.write(metaData.getColumnLabel(i)+"\t");
					pw.flush();
				}
				pw.write("\n");
				System.out.println(df.format(new Date())+" �ֶ���д�뵽�ļ��С�����"+sitename+sqlName);
				pwlog.write(df.format(new Date())+" �ֶ���д�뵽�ļ��С�����"+sitename+sqlName+"\n");
				pwlog.flush();
				while(rs.next())
				{
					for(int i=1;i<=metaData.getColumnCount();i++)
					{
						pw.write(rs.getString(i)+"\t");
						pw.flush();
					}
					pw.write("\n");
				}
				System.out.println(df.format(new Date())+" ���������ɹ�!"+sqlName);
				pwlog.write(df.format(new Date())+" ���������ɹ�!"+sitename+sqlName+"\n");
				pwlog.flush();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println(df.format(new Date())+" ���������ʧ��!"+sqlName);
				pwlog.write(df.format(new Date())+" ���������ʧ��!"+sitename+sqlName+"\n");
				pwlog.flush();
			}
		}
		System.out.println(df.format(new Date())+" ����ִ�н������ر����ݿ�����");
		pwlog.write(df.format(new Date())+" ����ִ�н������ر����ݿ�����"+sitename+sqlName+"\n");
		pwlog.flush();
		try {
			pw.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//ִ��Sql��䲢�õ����
	public static ResultSet execsql(Connection conn,String sql){
		
		Statement stsm=null;
		ResultSet rs=null;
		try {
			stsm=conn.createStatement();
			rs=stsm.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return null;
		}
		return rs;
	}	
	//��ȡ�ļ�,�����ļ�,���쳣
	public static HashMap<String,String> listfilename()
	{
		String sqlpath="C:\\Users\\enqrrwi\\Desktop\\Puerto Rico\\Puerto Rico\\07 SqlCommand\\";
		File file=new File(sqlpath);
		File[] filename=file.listFiles();
		FileInputStream ins=null;
		int countlen=0;
		byte[] m_binArray=null;
		HashMap<String,String> cmdinfo=new HashMap<String,String>();
		for(File i:filename){
			try {
				ins = new FileInputStream(i);
				countlen=ins.available();
				m_binArray=new byte[countlen];
				ins.read(m_binArray);
				String str=new String(m_binArray);
				cmdinfo.put(i.toString(), str);
				ins.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return cmdinfo;
	}
	
	//д���ļ�����־
	private static PrintWriter output(String filename,String sitename)
	{
		String path="C:/Users/enqrrwi/Desktop/Puerto Rico/Puerto Rico/05 Report_Rawdata/";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try{
			   FileOutputStream fos=new FileOutputStream(path+sitename+"_"+filename+".txt",true);//true������׷������
			   PrintWriter pw=new PrintWriter(fos);
			   return pw;
			   }catch(FileNotFoundException e){
			      e.printStackTrace();
			      System.out.println(df.format(new Date())+" "+filename+" д��ʧ�ܡ�������");

			      return null;
			   }
	}
	
	//������ݿ����Ӷ���
	private static Connection getconn()
	{
		Connection conn=null;
		String connectionUrl="jdbc:sybase:Tds:localhost:15000/dwhdb";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Class.forName("com.sybase.jdbc4.jdbc.SybDriver");
			conn=DriverManager.getConnection(connectionUrl, "dcbo", "dcbo");
			return conn;
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(df.format(new Date())+" û�л�ȡ�����Ӷ���,����CRT���ӣ�");
			System.exit(0);
			return null;		
		}
		
	}

}
