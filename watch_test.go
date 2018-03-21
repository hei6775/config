package gotest

import (
	"time"
	"log"
	"encoding/json"
	"testing"
	"sync"
)
//测试所用数据
var data =[]byte(`{
    "Title": "go programming language",
    "Author": "Dianchu",
    "Publisher": "qinghua",
    "IsPublished": true,
    "Price": 99
}`)
//测试所用数据解析的结构体
var datastruct struct{
	Title string `json:"title"`
	Author string `json:"author"`
	Publisher string `json:"publisher"`
	IsPublished bool  `json:"ispublished"`
	Price int `json:"price"`
}

var ch_config struct{
	Name string `json:"name"`
	App_id int `json:"app_id"`
}
var global_config struct{
	Compress bool `json:"compress"`
	Compress_a string `json:"compress_a"`
}

var hosts =[]string{"192.168.5.92:2181"}
var path1 = "/cnn"
var path2 = "/2"
var path3 = "/parent"
var sessiontime = time.Second * 10
var wg *sync.WaitGroup


//-----------方法测试------------
//测试连接和数据加载
func Test_Connect_and_getdata(t *testing.T){
	f := NewConfigW()
	err := f.Connect(hosts,sessiontime)
	defer f.Close()
	if err != nil {
		t.Error("测试失败",err)
	}
	data,err1 := f.Getdata(path1)
	if err1 != nil {
		t.Error("测试失败",err1)
	}
	t.Log("测试成功,节点数据为：",string(data))
}

//节点创建删除监听
// func Test_CDWatch(t *testing.T){
// 	f := NewConfigW()
// 	err := f.Connect(hosts,sessiontime)
// 	defer f.Close()
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	err = f.CDWatch(path2)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }

// //子节点变化监听
// func Test_ChildrenWatch(t *testing.T){
// 	f := NewConfigW()
// 	err := f.Connect(hosts,sessiontime)
// 	defer f.Close()
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	err = f.ChildrenWatch(path3)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }


//WatchC函数测试
// func Test_WatchC(t *testing.T){
// 	f1 := NewConfigW()
// 	err1 := f1.Connect(hosts,sessiontime)
// 	defer f1.Close()
// 	if err1 != nil {
// 		t.Error(err1)
// 	}

// 	path11 := []string{"/config_test/demo_1/global_config","/config_test/demo_1/ch_config/1010",
// 		"/config_test/demo_1/ch_config/1011"}
// 	err1 = f1.GetWatch(path11,callback1)
// 	if err1 != nil {
// 		t.Error(err1)
// 	}
// }

//WatchAll函数测试
func Test_WatchAll(t *testing.T){
	f := NewConfigW()
	err := f.Connect(hosts,sessiontime)
	defer f.Close()
	if err != nil {
		t.Error(err)
	}
	//兄弟节点
	pathlist := []string{"/config_test/demo_1/global_config"}
	err = f.WatchALL("/config_test/demo_1/ch_config", pathlist, callback1)
	if err != nil {
		t.Error(err)
	}
}

//私有
//----------------------------

//callback1
func callback1(data []byte,path string){
	f_json(data,path)
}

//解析到datastruct结构体中
func f_json(data []byte,path string){
	if path == "/config_test/demo_1/ch_config/1010" || path =="/config_test/demo_1/ch_config/1011"{
		err := json.Unmarshal(data, &ch_config)
		if err != nil{
			log.Println(err)
		}
		log.Printf("%+v", ch_config)
	}else if path == "/config_test/demo_1/global_config"{
		err := json.Unmarshal(data, &global_config)
		if err != nil{
			log.Println(err)
		}
		log.Printf("%+v",global_config)
	}
}
