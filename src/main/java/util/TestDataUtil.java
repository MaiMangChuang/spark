package util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


/**
 * 注意：车牌号码相同但车牌颜色不同 这代表着两种不同的车型，不同的号牌种类
 * 测试数据生成类
 * id,号牌号码,号牌种类,号牌颜色,车辆品牌,车辆颜色,通过时间,卡口编号,图片地址,卡口经纬度坐标
 * ID,HPHM,HPZL,HPYS,CLPP,CLYS,TGSJ,KKBH,TPDZ,KK_LON_LAT
 * @author brave
 */
public class TestDataUtil {

	public static void main(String[] args){
//		printInfile("ID,HPHM,HPZL,HPYS,CLPP,CLYS,TGSJ,KKBH,TPDZ,KK_LON_LAT");
		for(int i=0;i<1000;i++){
			double mLat1 = 34.670271; // point1纬度
			double mLon1 = 114.035958; // point1经度
			double mLat2 = 34.652931;// point2纬度
			double mLon2 = 114.035958;// point2经度
			String lonLat=MapUtil.randomLonLat(111.177463, 117.101392, 22.569001, 25.558735);
			printInfile(getRandomNum()+","+getHPHM()+","+getHPZL()+","+getHPYS()+","+getCLPP()+","+getCLYS()+","+randomDate()+","+"kk"+getRandomNum()+",,"+lonLat);
		}

		double subHour = getSubHour("2018-03-18 09:35:00", "2018-03-18 09:40:00");
		System.out.println(subHour);
	}
	/**
	 * 获取车辆颜色
	 * @return
	 */
	private static String getCLYS() {
		String[] clys=new String[]{"白","红","黑","灰","蓝"};
		String clysValue=clys[new Random().nextInt(5)];
		return clysValue;
	}
	/**
	 *  蓝底白字：小型车
	   黄底黑字：大型车
	   黑底白字：外籍车、外资企业、合资企业的车
	   红黑字：部队、武警、公、法、检、司法的车
	 * 获取号牌颜色
	 * @return
	 */
	private static String getHPYS() {
		String[] clys=new String[]{"蓝底白字","黄底黑字","黑底白字","红黑字"};
		String clysValue=clys[new Random().nextInt(4)];
		return clysValue;
	}
	/**
	 * 获取车辆品牌
	 * @return
	 */
	private static String getCLPP() {
		String[] clpp=new String[]{"本田","大众","起亚","丰田","斯卡达","五菱"};
		String clppValue=clpp[new Random().nextInt(6)];
		return clppValue;
	}
	/**
	 * 获取随机数，用作数据id字段值
	 * @return
	 */
	private static int getRandomNum(){
		int intFlag = (int)(Math.random() * 1000000);
		String flag = String.valueOf(intFlag);
		return intFlag;
	}
	/**
	 * 获取号牌种类
	 * @return
	 */
	private static String getHPZL(){
		String[] hpzl=new String[]{"大型汽车","小型汽车","教练车","警用汽车"};
		String hpzlValue=hpzl[new Random().nextInt(4)];
//		System.out.println(hpzlValue);
		return hpzlValue;
	}
	/**
	 * 获取号牌号码
	 * @return
	 */
	private static String getHPHM(){
		String[] shengShi=new String[]{"粤A","粤B","粤C","粤D","粤E","粤F","粤G","粤H"};
		String shengShiValue=shengShi[new Random().nextInt(8)];
		int count = 0;
		String carNo = "";
		while(count<5){
			long time = System.currentTimeMillis();
			Random random = new Random(time);
			String str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			char str2 = str.charAt(random.nextInt(26));
			int num = random.nextInt(10);
			//字母与数字的概率相同
			if (num<5) {
				carNo += num;
			}
			else {
				carNo += str2;
			}
			count++;
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return (shengShiValue+carNo);
	}
	/**
	 * 获取时间 格式为：yyyy-MM-dd hh:mm:ss
	 *
	 * @return
	 */
	private static String randomDate(){
		Random rndYear=new Random();
		int year=rndYear.nextInt(3)+2015;
		Random rndMonth=new Random();
		int month=rndMonth.nextInt(12)+1;
		Random rndDay=new Random();
		int Day=rndDay.nextInt(30)+1;
		Random rndHour=new Random();
		int hour=rndHour.nextInt(23);
		Random rndMinute=new Random();
		int minute=rndMinute.nextInt(60);
		Random rndSecond=new Random();
		int second=rndSecond.nextInt(60);
		return year+"-"+cp(month)+"-"+cp(Day)+" "+cp(hour)+":"+cp(minute)+":"+cp(second);
	}
	private static String cp(int num){
		String Num=num+"";
		if (Num.length()==1){
			return "0"+Num;
		}else {
			return Num;
		}
	}
	/**
	 * 将content写入文件
	 * @param content
	 */
	protected static void printInfile(String content) {
		String file = "traffic.csv";
		BufferedWriter out = null;
		try {
			System.out.println(content);
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			out.write(content);
			out.write("\r\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(out != null){
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 获取两个时间相差的分钟数 保留三位小数
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static double getSubMinutes(String date1,String date2){
		long date1Millis=getTimeMillis(date1);
		long date2Millis=getTimeMillis(date2);
		long sub=Math.abs(date1Millis-date2Millis);
		float subMinute = (float)sub/(1000*60);
		//保留三位小数，单位:分钟
		return (double)Math.round(subMinute*1000)/1000;
	}

    /**
     * 获取两个时间相差的小时数 保留三位小数
     * @param date1
     * @param date2
     * @return
     */
	public static double getSubHour(String date1,String date2){
    	long date1Millis=getTimeMillis(date1);
    	long date2Millis=getTimeMillis(date2);
    	long sub=Math.abs(date1Millis-date2Millis);
    	float subHour = (float)sub/(1000*60*60);
    	//保留三位小数，单位:小时
    	return (double)Math.round(subHour*1000)/1000;
    }
    /**
     * @param dateStr 时间格式：yyyy-MM-dd hh:mm:ss
     * @return 返回时间对应的毫秒数
     * @throws ParseException 
     */
    private static long getTimeMillis(String dateStr) {
    	long timeMillis=0;
		try {
			DateFormat fmt =new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			Date date = fmt.parse(dateStr);
			timeMillis = date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
        return timeMillis;
    }
    /**
     * 计算速度
     * @param distance
     * @param hour
     * @return返回速度值 单位km/h
     */
	public static Integer getSpeed(double distance,double hour){
    	if(hour==0){
    		return null;
    	}
    	return (int)(distance/hour);
    }
    
}
