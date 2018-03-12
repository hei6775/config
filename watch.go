package main

import (
	"fmt"
	"time"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"log"
)


type Config interface{
	Close()			//关闭连接,使用时需使用defer
	CreateZnode()	//创建指定的节点，默认永久节点 flag:0 永久；1：短暂；2：永久有序；3：短暂有序
	Delete()		//删除指定的节点
	Getdata()		//得到指定znode数据
	GetChild()		//得到子节点
	Exist()			//判断指定节点存在状态
	SetData()		//设置数据
	ExistWatch()	//存在状态监听
	ChildWatch()	//子节点数量变化监听	
	GetWatch()		//指定节点监听,callback需要调用者自己定义
	WatchDemoNode() //节点及其子节点监听，callback需要调用者自己定义

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
				fmt.Println(err)
			}
			fmt.Printf("The %v create OK", path)
	case 2:path, err := m.conn.Create(path, data, zk.FlagSequence, acls)
		if err != nil{
			fmt.Println(err)
		}
		fmt.Printf("The %v create OK", path)
	case 3:path, err := m.conn.Create(path, data, 3, acls)
		if err != nil{
			fmt.Println(err)
		}
		fmt.Printf("The %v create OK", path)
	default:path, err := m.conn.Create(path, data, 0, acls)
		if err != nil{
			fmt.Println(err)
		}
		fmt.Printf("The %v create OK", path)
	}
}

//删除节点，暂时无法递归删除
func (m *Connect)Delete(path string){
	err := m.conn.Delete(path, -1)
	if err != nil{
		fmt.Printf("The %v: %v", path,err)
	}
	fmt.Printf("The %v was deleted successful.", path)
}

//得到指定节点内的数据
func (m *Connect)Getdata(path string)[]byte{
	data, _, err := m.conn.Get(path)
	must(err)
	return data
}

//得到指定节点下的子节点
func (m *Connect)GetChild(path string)[]string{
	child, _, err :=m.conn.Children(path)
	if err != nil{
		fmt.Println(err)
		return nil
	}
	fmt.Println(child)
	return child
}

//节点存在与否
func (m *Connect)Exist(path string){
	state, _, err := m.conn.Exists(path)
	must(err)
	fmt.Printf("The %s state: %v.", path, state)
}

//修改数据
func (m *Connect)Setdata(path string, data []byte){
	_, err := m.conn.Set(path, data, -1)
	must(err)
	fmt.Println("The %s's data was setted successful.", path)
}

//节点增删监听
func (m *Connect)ExistWatch(path string)(){
	for{
		State_Bool, _, ech, err := m.conn.ExistsW(path)
		must(err)	
		fmt.Printf("The %s exists state: %v.", path, State_Bool)
		event :=<- ech
		fmt.Println("*******************")
		fmt.Println("path:", event.Path)
		fmt.Println("type:", event.Type.String())
		fmt.Println("state:", event.State.String())
		fmt.Println("-------------------")
	}
}

//子节点监听
func (m *Connect)GetWatchChild(path string,callback func([]byte)){
	for{
		_, _, ech, err := m.conn.ChildrenW(path)
		must(err)
		event :=<- ech
		fmt.Println("*******************")
		fmt.Println("path:", event.Path)
		fmt.Println("type:", event.Type.String())
		fmt.Println("state:", event.State.String())
		fmt.Println("-------------------")
	}
}

//指定节点的监听
func (m *Connect)GetWatch(path string,callback func([]byte))(){
	for {
		_, _, ech, err := m.conn.GetW(path)
		must(err)
		event :=<- ech
		switch event.Type{
		case zk.EventNodeDataChanged:
			data, _, err := m.conn.Get(path)
			must(err)
			callback(data)
		case zk.EventNodeDeleted:
			m.conn.Close()
		default:
			fmt.Println("*******************")
			fmt.Println("path:", event.Path)
			fmt.Println("type:", event.Type.String())
			fmt.Println("state:", event.State.String())
			fmt.Println("-------------------")
		}
	}
}

//节点及其子节点监听
func (m *Connect)WatchDemoNode(path string,callback2 func([]byte)) {

    //创建
    go watchNodeCreated(path, m.conn)
    //改值
    go watchNodeDataChange(path, m.conn,callback2)
    //子节点数量变化「增删」
	go watchNodeDataChange2(path, m.conn,callback2)
	//(新)子节点数量监听
    go watchChildrenChanged(path, m.conn)
    //删除节点
    watchNodeDeleted(path, m.conn)

}

//私有
//-----------------------------------------------
func watchNodeCreated(path string, conn *zk.Conn) {
    log.Println("watchNodeCreated")
    for {
        _, _, ch, err := conn.ExistsW(path)
		e := <-ch
		must(err)
        log.Println("ExistsW:", e.Type, "Event:", e)
        if e.Type == zk.EventNodeCreated {
            log.Println("NodeCreated ")
            return
        }
    }
}

func watchNodeDeleted(path string, conn *zk.Conn) {
    log.Println("watchNodeDeleted")
    for {
		_, _, ch, err := conn.ExistsW(path)
		must(err)
        e := <-ch
        log.Println("ExistsW:", e.Type, "Event:", e)
        if e.Type == zk.EventNodeDeleted {
            log.Println("NodeDeleted ")
            return
        }
    }
}

func watchNodeDataChange(path string, conn *zk.Conn,callback2 func([]byte)) {
    for {
        _, _, ch, err1 := conn.GetW(path)
		must(err1)
		e := <-ch
		log.Println("GetW('"+path+"'):", e.Type, "Event:", e)
		if e.Type == zk.EventNodeDataChanged {
			data, _, err2 := conn.Get(path)
			must(err2)
			callback2(data)
			
		}
    }
}

func watchNodeDataChange2(path string, conn *zk.Conn,callback2 func([]byte)){
    log.Println("children watchchange")
    path_list, _,err := conn.Children(path)
	must(err)
    for i,v := range path_list {
        path_list[i] = path+"/"+v
    }
    for _, vv := range path_list{
        go watchNodeDataChange(vv, conn,callback2)
    }
}


func watchChildrenChanged(path string, conn *zk.Conn) {
    for {
		path_list, _, ch,err := conn.ChildrenW(path)
		must(err)
		fmt.Println(path_list)
        e := <-ch
        log.Println("The changed path:",e.Path,"ChildrenW:", e.Type, "Event:", e)
    }
}

func must(err error)(){
    if err != nil {
        panic(err)
    }
}

//--------------------------------------------------------
