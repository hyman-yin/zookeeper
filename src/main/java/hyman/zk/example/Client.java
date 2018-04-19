package hyman.zk.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import hyman.zk.constants.ConnectInfoConstant;

import org.apache.zookeeper.ZooKeeper;

/**
 * 客户端，监听服务器的节点增删事件
 * @author hyman
 *
 */
public class Client {
	@SuppressWarnings("unused")
	private volatile List<String> servers = null;
	
	private ZooKeeper zk = null;
	
	/**
	 * 获取连接
	 * @throws IOException
	 */
	private void getZKClient() throws IOException{
		zk = new ZooKeeper(ConnectInfoConstant.connectString, ConnectInfoConstant.sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				//获取服务器列表
				try {
					if(event.getType()==EventType.None){
						return;
					}
					
					updateServers();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	
	/**
	 * 获取服务器列表
	 * @throws Exception
	 */
	public synchronized void updateServers() throws Exception {
		
		List<String> list = zk.getChildren(ConnectInfoConstant.serverRootPath, true);
		
		List<String> serverList = new ArrayList<>();
		
		if(list!=null && list.size()>0){
			for(String child : list){
				byte[] data = zk.getData(ConnectInfoConstant.serverRootPath+"/"+child, false, null);
				serverList.add(new String(data));
			}
		}
		
		servers = serverList;
		
		if(serverList!=null && serverList.size()>0){
			for(String str : serverList ){
				System.out.println(str);
			}
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Client client = new Client();
		client.getZKClient();
		client.updateServers();
		client.handle();
	}
	
	
	public void handle() throws Exception{
		System.out.println("client do something... ");
		TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
	}
	
}
