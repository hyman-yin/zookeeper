package hyman.zk.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

/**
 * 测试工具类，演示zookeeper的基本增删改查操作
 * @author hyman
 *
 */
public class ZKUtils {
	private static final String connectString = "192.168.106.130:2181";
	private static final int sessionTimeout=2000;
	
	private ZooKeeper zk = null;
	
	@Before
	public void init() throws IOException{
		zk=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			/**
			 * 监听事件发生时的回调方法
			 */
			@Override
			public void process(WatchedEvent event) {
				if(event.getType()!=EventType.None){
					System.out.println("event type: "+event.getType());
					System.out.println("event path: "+event.getPath());
				}
				try {
					zk.getData("/hyman/test", true, null);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	
	/**
	 * 创建节点
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void createNode() throws KeeperException, InterruptedException{
		zk.create("/hyman/createbyjava", "created by java".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		zk.close();
	}
	
	
	/**
	 * 删除节点
	 * @throws Exception
	 */
	@Test
	public void deleteNode() throws Exception{
		zk.delete("/hyman/test", -1);
		System.out.println(zk.exists("/hyman/createbyjava", false));
	}
	
	
	/**
	 * 更新节点值
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws Exception
	 */
	@Test
	public void updateNode() throws KeeperException, InterruptedException {
		zk.setData("/hyman/test", "updated by java".getBytes(), -1);
	}
	
	
	/**
	 * 获取节点值
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void getNodeData() throws KeeperException, InterruptedException{
		System.out.println(zk.getData("/hyman/test", false, null));
	}
	
	
	@Test
	public void getChildren() throws KeeperException, InterruptedException{
		List<String> list = zk.getChildren("/hyman", false);
		if(list!=null && list.size()>0){
			for(String str : list) {
				System.out.println(str);
			}
		}
	}
	
	
	/**
	 * zk监听机制：
	 * 1，事先定义好监听的回调函数
	 * 2，对节点进行各种访问操作时可以注册监听
	 * 3，被监听的节点发生相应的事件时，zk客户端会接收到集群的事件通知
	 * 4，调用第一步定义好的回调函数
	 * 5，注意：监听事件只能启动一次，如果要持续监听，可以在监听的回调函数中把事件再调用一次,不同的事件监听不同的类型，
	 * 	具体的监听事件可以参考EventType类下的常量
	 * 
	 * @throws Exception
	 */
	@Test
	public void watch() throws Exception {
		//获取数据时注册监听（在init方法中）
		zk.getData("/hyman/test", true, null);
		
		//让程序保持运行
		TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
	}
	
	
	/**
	 * 上传配置文件到zookeeper，大小不要超过1M。否则会报错
	 * @throws Exception
	 */
	@Test
	public void fileUpload() throws Exception {
		String filePath="F:/redis.properties";
		String redis_properties = FileUtils.readFileToString(new File(filePath),"utf-8");
		
		zk.setData("/hyman/conf/redis_properties", redis_properties.getBytes(),-1);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
