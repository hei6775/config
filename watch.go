package config

import (
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"log"
)

type Config interface{
	Close()			//关闭连接,使用时需使用defer
	CreateZnode()		//创建指定的节点，默认永久节点 flag:0 永久；1：短暂；2：永久有序；3：短暂有序
	Delete()		//删除指定的节点
	Getdata()		//得到指定znode数据
	Children()		//得到子节点
	Exist()			//判断指定节点存在状态
	SetData()		//设置数据
    	CreateWatch()		//节点创建监听
    	DeleteWatch()   	//节点删除监听
	ChildrenWatch()		//子节点数量变化监听	
	GetWatch()		//指定节点监听,callback需要调用者自己定义

}

//Connect结构体
type Connect struct{
	conn *zk.Conn
}

//实例化一个Connect
func NewConn(host []string)Connect{
	conn1, _,err := zk.Connect(host, time.Second*10)
	if err != nil{
		panic(err)
	}
	connect1:= Connect{
		conn:conn1,
	}
	return connect1
}

//关闭连接
func (m *Connect)Close(){
	defer m.conn.Close()
}

//创建节点
func (m *Connect)CreateZnode(path string, data []byte, flags int){
	var acls=zk.WorldACL(zk.PermAll)
	switch flags{
	case 1:path, err := m.conn.Create(path, data, zk.FlagEphemeral, acls)
			if err != nil{
				log.Println(err)
			}
			log.Printf("The %v create OK", path)
	case 2:path, err := m.conn.Create(path, data, zk.FlagSequence, acls)
		if err != nil{
			log.Println(err)
		}
		log.Printf("The %v create OK", path)
	case 3:path, err := m.conn.Create(path, data, 3, acls)
		if err != nil{
			log.Println(err)
		}
		log.Printf("The %v create OK", path)
	default:path, err := m.conn.Create(path, data, 0, acls)
		if err != nil{
			log.Println(err)
		}
		log.Printf("The %v create OK", path)
	}
}

//删除节点，暂时无法递归删除
func (m *Connect)Delete(path string){
	err := m.conn.Delete(path, -1)
	if err != nil{
		log.Printf("The %v: %v", path,err)
	}
	log.Printf("The %v was deleted successful.", path)
}

//得到指定节点内的数据
func (m *Connect)Getdata(path string)[]byte{
	data, _, err := m.conn.Get(path)
	must(err)
	return data
}

//得到指定节点下的子节点
func (m *Connect)Children(path string)[]string{
	child, _, err :=m.conn.Children(path)
	if err != nil{
		log.Println(err)
		return nil
	}
	log.Println(child)
	return child
}

//节点存在与否
func (m *Connect)Exist(path string)bool{
	state, _, err := m.conn.Exists(path)
	if err != nil{
		log.Println(err)
	}
	log.Printf("The %s state: %v.", path, state)
	return state
}

//修改数据
func (m *Connect)Setdata(path string, data []byte){
	_, err := m.conn.Set(path, data, -1)
	must(err)
	log.Println("The %s's data was setted successful.", path)
}

//节点创建监听，对于节点数据修改也会引起eventDatechanged
func (m *Connect)CreateWatch(path string)(){
	for{
		_, _, ech, err := m.conn.ExistsW(path)
		must(err)	
        event :=<- ech
        log.Println("Create result:")
        log.Println("ExistsW:", event.Type, "Event:", event)
        if event.Type == zk.EventNodeCreated {
            log.Println("NodeCreated ")
            return
        }
	}
}

//节点删除监听，对于节点数据修改也会引起eventNodeDatechanged
func (m *Connect)DeleteWatch(path string)(){
	for{
		_, _, ech, err := m.conn.ExistsW(path)
		must(err)	
        event :=<- ech
        log.Println("DeleteWatch result:")
        log.Println("ExistsW:", event.Type, "Event:", event)
        if event.Type == zk.EventNodeDeleted {
            log.Println("NodeDeleted ")
            return
        }
	}
}

//子节点数量监听，过滤了eventNodeDataChanged
func (m *Connect)ChildrenWatch(path string){
	for{
		pathlist, _, ech, err := m.conn.ChildrenW(path)
        must(err)
        log.Println(pathlist)
        event :=<- ech
        if event.Type == zk.EventNodeDataChanged{
        }else{
            log.Println("E.Path:",event.Path,"E.Type:", event.Type, "E.State:", event.State)
        }
	}
}

//指定节点的数据监听，对于父节点而言，修改父节点会引起ChildrenW的watch
func (m *Connect)GetWatch(path string,callback func([]byte))(){
	for {
		_, _, ech, err := m.conn.GetW(path)
		must(err)
		event :=<- ech
		switch event.Type{
        case zk.EventNodeDataChanged:
            log.Println("E.Path:",event.Path,"E.Type:", event.Type, "Event:", event)
			data, _, err := m.conn.Get(path)
			must(err)
			callback(data)
        case zk.EventNodeDeleted:
            log.Println("E.Path:",event.Path,"E.Type:", event.Type, "Event:", event)
            log.Println("Now,the connect was closed")
			m.conn.Close()
		default:
			log.Println("path:", event.Path)
			log.Println("type:", event.Type.String())
			log.Println("state:", event.State.String())
		}
	}
}

//私有
//-----------------------------------------------
func must(err error)(){
    if err != nil {
        panic(err)
    }
}
