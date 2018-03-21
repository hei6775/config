package gotest

import (
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"time"
	"errors"
)

var PARA_ZERO = errors.New("The path is null,please check your parameter.")
var PARA_CHILDZERO = errors.New("Your children path is null.")

//配置信息结构体
//------------------------------------------------
type IpList []string

// 服务地址映射，请求方式->服务地址
type serverUrlConf struct {
	HTTP string `json:"HTTP,omitempty"`
	GRPC string `json:"GRPC,omitempty"`
}
// globalConf 全局设置
type globalConf struct {
	ForwardTimeout          int    `json:"forward_timeout"`
	SessionTimeout          int    `json:"session_timeout"`
	UserCenterTimeout       int    `json:"user_center_timeout"`
	OverTimeout             int    `json:"over_timeout"`
	CheckOverTime           bool   `json:"check_over_time"`
	CheckIp                 bool   `json:"check_ip"`
	LogLevel                string `json:"log_level"`
	LogFixedFlag            bool   `json:"log_fixed_flag"`
	SessionServerMethod     string `json:"session_server_method"`                 // 会话请求方式设置
	SessionHTTPKeepAlive    bool   `json:"session_http_keep_alive"`               // 会话HTTP长连接设置
	UserCenterMethod        string `json:"user_center_method"`                    // 用户中心请求方式设置
	UserCenterHTTPKeepAlive bool   `json:"user_center_http_keep_alive,omitempty"` // 用户中心HTTP长连接接设置
	ForwardHTTPKeepAlive    bool   `json:"forward_http_keep_alive,omitempty"`     // 后端服务HTTP长连接设置

	UserCenter          serverUrlConf `json:"user_center,omitempty"`    // 用户中心地址配置
	SessionServer       serverUrlConf `json:"session_server,omitempty"` // 会话服务地址配置
	MonitorServer       string        `json:"monitor_server"`           // 监控服务地址
	FlowLimitRate       int           `json:"flow_limit_rate"`          // 请求限流速率设置(每秒多少个请求)
	HandleRequestNumber int           `json:"handle_request_number"`    // 并发请求相对限流设置比例（服务支持同时处理的请求数：限流值x该值）
	WhiteIpList         IpList        `json:"white_ip_list"`
}

// DistConf 单项分发操作配置
type DistConf struct {
	ContentType string `json:"content_type"` // 请求头设置
	Method      string `json:"method"`       // 请求方式
	Compression bool   `json:"compression"`  // 是否压缩
	Encryption  bool   `json:"encryption"`   // 是否加密
	Address     string `json:"address"`      // 请求分发地址
	Log         bool   `json:"log"`          // 是否记录日志
}

// StatisticsConf 统计配置
type StatisticsConf struct {
	Background    bool    `json:"background"`     // 是否启用后台记录
	Log           bool    `json:"log"`            // 是否日志输出
	AuthKey       string  `json:"auth_key"`       // API验证key
	LogInterval   int     `json:"log_interval"`   // 日志输出间隔时间，秒级
	LogRecent     int     `json:"log_recent"`     // 日志统计最近几个step的数据，默认是0，表示全部
	LogPercentile float64 `json:"log_percentile"` // 日志统计百分位数据，默认是50.0
	Size          int     `json:"size"`           // 记录器容器大小，
	Step          int     `json:"step"`           // 记录器存储容量大小，秒级。设置为10表示每个记录容器存储10秒的内数据，记录器总容量为step*size,默认使用60*1(1分钟)
}

// FrontendSetting 连接池总配置
type FrontendSetting struct {
	GlobalConfig     globalConf          `json:"global_config"` // 全局配置
	DistributeConfig map[string]DistConf `json:"distribute"`    // 分发操作配置
}
//-----------------------------------


type ConfigW interface{
	//连接zk
	Connect(hosts []string,sessiontime time.Duration) (error)
	//关闭连接,使用时需使用defer		
	Close()
	//得到指定znode数据					
	Getdata(path string)([]byte,error)
	//节点创建删除监听		
	CDWatch(path string)(error)
	//子节点数量变化监听			
	ChildrenWatch(path string)(error)
	//指定节点监听,callback需要调用者自己定义			
	GetWatch(path []string,	callback func(data []byte,changepath string))(error)
	//进行Node下未知子节点的监听，需要将NewConn的ech传递给WatchC		
	WatchC(path string,	callback func(data []byte,changepath string))(error)		
	//进行指定节点pathlist和path下子节点的监听方法
	WatchALL(path string,pathlist []string,callback func(data []byte,changepath string))(error)
}

//Connect结构体
type connect struct{
	conn *zk.Conn	//生成的连接
	data []byte		//数据
	err error		//错误
	ech <-chan zk.Event //连接过程产生的ech
	exist_flag bool		//节点存在标志
	childrenlist []string //子节点列表
}

//实例化一个接口
func NewConfigW() ConfigW{
	var Conf ConfigW
	connect_ := new(connect)
	Conf = connect_
	return Conf
}

//开始连接
func (c *connect) Connect(host []string,sessiontime time.Duration)(error){
	conn1,ech,err := zk.Connect(host, sessiontime)
	if err != nil{
		c.err = err
		return err
	}
	c.conn = conn1
	c.ech = ech
	return nil 
}

//关闭连接
func (c *connect)Close(){
	c.conn.Close()
}


//加载指定节点内的数据
func (c *connect)Getdata(path string)([]byte, error){
	data, _, err := c.conn.Get(path)
	if err != nil{
		log.Println(err)
		return nil,err
	}
	c.data = data
	log.Println(string(c.data))
	return c.data,nil
}

//节点创建删除监听，对于节点数据修改也会引起eventDatechanged
func (c *connect)CDWatch(path string)(error){
	for{
		flag, _, ech, err := c.conn.ExistsW(path)
		if err != nil{
			return err
		}
		c.exist_flag = flag
		log.Println("exist state: ",c.exist_flag)
        event :=<- ech
		log.Println("Watch result:")
		if event.Type == zk.EventNodeCreated {
            log.Println("This is an event about NodeCreated. ")
        }else if event.Type == zk.EventNodeDeleted{
			log.Println("This is an event about NodeDeleted.")
		} 
        log.Println("E.Type:",event.Type,"E.Path:",event.Path,"E.State",event.State)
	}
}

//子节点数量监听，对于ChildrenW而言，修改Path数据会触发EventNodeDataChanged事件
//但是对于Path的子节点的数据的修改而言，是不会触发EventNodeDataChanged事件的
func (c *connect)ChildrenWatch(path string)(error){
	for{
		pathlist, _, ech, err := c.conn.ChildrenW(path)
		if err != nil{
			return err
		}
		c.childrenlist = pathlist
		log.Println(c.childrenlist)
        event :=<- ech
        if event.Type == zk.EventNodeDataChanged{
			log.Println("This is an event about NodeDataChange")
        }else{
            log.Println("E.Type:", event.Type,"E.Path:",event.Path, "E.State:", event.State)
        }
	}
}

//指定节点的数据监听,在node下设置GetW，子节点的修改数据和创建删除都不会触发该GetW设置的watch
func (c *connect)GetWatch(path []string,callback func(data []byte,changepath string))(error){
	num := len(path)
	if num < 1{
		return PARA_ZERO
	}
	for _,v := range path{
		_,_,_,err := c.conn.GetW(v)
		if err != nil{
			return err
		}
	}
	//清空通道信息
	clearFlag := false
	for !clearFlag {
		select {
		case <- c.ech:
		default:
			clearFlag = true
		}
	}
	for{
		select{
		case e :=<-c.ech:{		
			log.Println("E.Type",e.Type,"E.Path:",e.Path,"E.State",e.State)
			//路径、事件检查
			flag := check_path_event(e.Path,e.Type, path)
			if flag{
					//得到修改后的数据and重新设置Watch
					data, _,_, err2 := c.conn.GetW(e.Path)
					c.data = data
					if err2 != nil{
						return err2
					}
					callback(data,e.Path)//callback处理数据
				}else {
				log.Println("Sorry,this is not the event that you want to get.")
				}
			}
		}
	}
}


// 监听node下的多个子节点
func (c *connect)WatchC(path string,callback func([]byte,string))(error){
	//获取path下子节点
	childrenlist, _, err := c.conn.Children(path) 
	if err != nil{
		return err
	}
	if len(childrenlist) < 1 {
		return PARA_CHILDZERO
	}
	//循环设置Watch
	for i,v  := range childrenlist{
		childrenlist[i] = path+"/"+v
		_, _, _, err = c.conn.GetW(childrenlist[i])
		if err != nil {
			return err
		}
	}
	//清空，ech会在连接时产生消息，有三个（state:StateConnecting,StateConnected,StateHasSession）
	clearFlag := false
	for !clearFlag {
		select {
		case <- c.ech:
		default:
			clearFlag = true
		}
	}
	for{
		select{
		case e1:=<- c.ech:{
			log.Println("E.Type:",e1.Type,"E.Path:",e1.Path,"E.State",e1.State)
			flag := check_path_event(e1.Path,e1.Type, childrenlist)
			if flag{
				data, _,_, err2 := c.conn.GetW(e1.Path)
				c.data = data
				if err2 != nil{
					return err2
				}
				//callback处理数据,参数分别为修改后数据以及修改的节点
				callback(data,e1.Path)
			}else {
				log.Println("Sorry,this is not the event that you want to get.")
			}
			}
		}
	}
}

//指定节点监听以及指定节点的子节点监听
func (c *connect)WatchALL(path string,pathlist []string,callback func(data []byte,changepath string))(error){
	if len(pathlist)<1{
		return PARA_ZERO
	}
	childrenlist, _, err := c.conn.Children(path) //获取path下子节点
	if err != nil {
		return err
	}else if len(childrenlist) <1 {
		return PARA_CHILDZERO
	}
	//为子节点设置Watch
	for i,v  := range childrenlist{					
		childrenlist[i] = path+"/"+v
		_, _, _, err = c.conn.GetW(childrenlist[i])
		if err != nil {
			return err
		}
	}
	//为pathlist设置Watch
	for _,v  := range pathlist{						
		_, _, _, err = c.conn.GetW(v)
		if err != nil{
			return err
		}
	}
	//清空，ech会在连接时产生消息，有三个（state:StateConnecting,StateConnected,StateHasSession）
	clearFlag := false
	for !clearFlag {
		select {
		case <- c.ech:
		default:
			clearFlag = true
		}
	}
	for{
		select{
		case e1:=<- c.ech:{
			log.Println("E.Type",e1.Type,"E.Path:",e1.Path,"E.State",e1.State)
			//子节点标志flag1，pathlist标志flag2
			flag1 := check_path_event(e1.Path,e1.Type, childrenlist)
			flag2 := check_path_event(e1.Path,e1.Type, pathlist)
			if flag1 || flag2{
				//重新设置watch
				data, _, _,err2 := c.conn.GetW(e1.Path)
				if err2 != nil {
					return err2
				}
				//callback处理数据
				callback(data,e1.Path)
			}else {
				log.Println("Sorry,this is not the event that you want to get.")
			}
			}
		}
	}
}

//私有
//-----------------------------------------------

func check_path_event(epath string,event zk.EventType,list []string)(flag1 bool){
	flag1 = false
	for _,v := range list{
		if epath == v && event== zk.EventNodeDataChanged{
			flag1 =true
			return
		}
	}
	return
}


//--------------------------------------------------------
