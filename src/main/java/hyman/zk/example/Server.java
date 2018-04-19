package hyman.zk.example;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import hyman.zk.constants.ConnectInfoConstant;

import org.apache.zookeeper.ZooKeeper;

/**
 * 服务器端
 * 可以把项目导出成可执行的jar包，server --> server.jar， client --> client.jar。
 * 这样可以实现分布式的效果
 * @author hyman
 *
 */
public class Server {
	private ZooKeeper zk = null;
	
	/**
	 * 获取连接
	 * @throws IOException
	 */
	private void getZKClient() throws IOException{
		zk = new ZooKeeper(ConnectInfoConstant.connectString, ConnectInfoConstant.sessionTimeout, null);
	}
	
	
	/**
	 * 向zookeeper注册服务器信息，即在/servers下创建子节点
	 * serverName: server name
	 * port : server listen port
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public void register(String serverName,String port) throws Exception{
		//连接ZK，创建节点
		zk.create(ConnectInfoConstant.serverRootPath+"/", (serverName+":"+port).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("server "+serverName+" is online...");
	}
	
	
	public static void main(String[] args) throws Exception {
		Server server = new Server();
		server.getZKClient();
		server.register(args[0],args[1]);
		
		server.handle(args[0]);
	}
	
	
	public void handle(String serverName) throws Exception {
		System.out.println("server "+serverName+" print hello world! ");
		TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
	}
}
