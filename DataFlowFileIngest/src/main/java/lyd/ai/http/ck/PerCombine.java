package lyd.ai.http.ck;

import lyd.ai.http.ck.client.ClickHouseClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import lyd.ai.http.ck.client.ClickHouseClient;

public class PerCombine {
	private static ClickHouseClient client = new ClickHouseClient("http://10.127.7.11:8123", "default", "lyd.ai");
	private static final DateTimeFormatter timeF1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	private static final DateTimeFormatter timeF2 = DateTimeFormatter.ofPattern("yyyyMMdd");
	
	
	private static final DateTimeFormatter timeF3 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	public static List<Object[]> generateRowsList(String file) throws IOException {
		
		LocalDateTime indexDate = LocalDateTime.parse(LocalDateTime.now().format(timeF1), timeF1);
		
		List<Object[]> rows = new ArrayList<>();
		List<Object[]> rowToo = new ArrayList<>();
		List<Object[]> rowTToo = new ArrayList<>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file)), "UTF-8"));
		String lineTxt = null;
		while ((lineTxt = br.readLine()) != null) {
			String[] lines = lineTxt.split("\001");
			if (lines.length == 17 ||lines[14].length()==14) {
				 String euAccessDate = LocalDateTime.parse(lines[14], timeF1).format(timeF3);
				 Object[] SS = { lines[1], // url
								 lines[3], // host
								 lines[4], // topDomain
								 lines[6], // ipaddr
								 lines[7], // country
								 lines[8], // province
								 lines[9], // city
								 lines[11], // ispID
								 lines[12], // audioVideoType
								 euAccessDate, // euAccessDate
								 lines[14] }; // euAccessTime
				if (rows.size() < 5000000) {
					rows.add(SS);
				} else if (rowToo.size() < 5000000) {
					rowToo.add(SS);
				} else {
					rowTToo.add(SS);
				}

			}
		}
		
		client.post("INSERT INTO default.white_grey_all_" +indexDate.format(timeF2), rows);
		if (rowToo.size() != 0) {
			client.post("INSERT INTO default.white_grey_all_" +indexDate.format(timeF2), rowToo);
		} else if (rowTToo.size() != 0) {
			client.post("INSERT INTO default.white_grey_all_" + indexDate.format(timeF2), rowTToo);
		}
		System.err.println("rows  >>>>>>>>>>>>        " + rows.size());
		System.err.println("rowToo  >>>>>>>>>>        " + rowToo.size());
		System.err.println("rowTToo  >>>>>>>>>        " + rowTToo.size());
		br.close();
//		client.close();
		return rows;
	}

	public static String getLocalIP() throws UnknownHostException {
		 InetAddress addr = InetAddress.getLocalHost();   
		 return addr.getHostAddress().toString();
		
	}
	public static void main(String[] args) {
		System.err.println(LocalDateTime.parse("20180613112312", timeF1).format(timeF3));
	}
}
