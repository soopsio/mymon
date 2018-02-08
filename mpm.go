// MySQL Performance Monitor(For open-falcon)
// Write by Li Bin<libin_dba@xiaomi.com>
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	// goconf "github.com/akrennmair/goconf"
	"github.com/go-ini/ini"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

type Cfg struct {
	LogFile      string
	LogLevel     int
	FalconClient string
	Endpoint     string
	MyCnf        string
	KafkaBrokers string
	KafkaTopic   string
	MyDef        MysqlIns
	MysqlIns     map[string]MysqlIns
}

var cfg Cfg
var localip net.IP

// 取网卡的默认ipv4 地址（第一个地址）
func getDefaultIP4(eth string) (net.IP, error) {
	addrs, err := getIP(eth)
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		// 轮询解析后的地址，如果是ipv4 ，则直接return
		ip, _, _ := net.ParseCIDR(addr.String())
		if i := ip.To4(); i != nil {
			return ip, nil
		}
	}
	return nil, errors.New("Can not find a valid ip")
}

// 获取指定接口(网卡)的地址列表
func getIP(eth string) ([]net.Addr, error) {
	i, err := net.InterfaceByName(eth)
	if err != nil {
		return nil, err
	}
	return i.Addrs()
}

// 获取本地默认ip
func defaultIP() (net.IP, error) {
	conn, err := net.Dial("udp", "www.google.com:80")
	if err != nil {
		// fmt.Println(err.Error())
		return nil, err
	}
	defer conn.Close()
	return net.ParseIP(strings.Split(conn.LocalAddr().String(), ":")[0]), nil
}

func init() {
	var cfgFile, cfgInterface string
	flag.StringVar(&cfgFile, "c", "myMon.cfg", "myMon configure file")
	flag.StringVar(&cfgInterface, "i", "default", "Local network interface")
	flag.Parse()

	if _, err := os.Stat(cfgFile); err != nil {
		if os.IsNotExist(err) {
			log.WithField("cfg", cfgFile).Fatalf("myMon config file does not exists: %v", err)
		}
	}

	if err := cfg.readConf(cfgFile); err != nil {
		log.Fatalf("Read configure file failed: %v", err)
	}

	// 获取本地IP
	if ip, err := getDefaultIP4(cfgInterface); err == nil {
		localip = ip
	} else {
		localip, _ = defaultIP()
	}

	// Init log file
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.Level(cfg.LogLevel))

	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err == nil {
			log.SetOutput(f)
			return
		}
	}
	log.SetOutput(os.Stderr)
}

//获取指定目录下的所有文件，不进入下一级目录搜索，可以匹配后缀过滤。
func ListDir(dirPth string, suffix string) (files []string, err error) {
	files = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}
	PthSep := string(os.PathSeparator)
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写
	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}
		if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) { //匹配文件
			files = append(files, dirPth+PthSep+fi.Name())
		}
	}
	return files, nil
}

func (conf *Cfg) readConf(file string) error {
	c, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true, IgnoreContinuation: true}, file)
	if err != nil {
		return err
	}
	section, err := c.GetSection("default")
	if err != nil {
		return err
	}

	conf.LogFile = section.Key("log_file").String()

	conf.LogLevel = section.Key("log_level").RangeInt(5, 0, 5)

	conf.FalconClient = section.Key("falcon_client").String()

	conf.Endpoint = section.Key("endpoint").String()

	conf.MyCnf = section.Key("mycnf").String()

	kafka, err := c.GetSection("kafka")
	if err != nil {
		return err
	}
	conf.KafkaBrokers = kafka.Key("brokers").String()
	conf.KafkaTopic = kafka.Key("topic").String()

	// 读取默认配置
	mysqlsection, err := c.GetSection("mysqld")
	if err != nil {
		return err
	}
	conf.MyDef.User = mysqlsection.Key("user").String()

	conf.MyDef.Pass = mysqlsection.Key("password").String()

	conf.MyDef.Host = mysqlsection.Key("host").String()

	conf.MyDef.Socket = mysqlsection.Key("socket").String()

	conf.MyDef.Port, err = mysqlsection.Key("port").Int()
	if err != nil {
		return err
	}

	sections := c.Sections()

	instancecfgs, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true, UnescapeValueDoubleQuotes: true}, conf.MyCnf)
	if err == nil {
		for _, v := range instancecfgs.Sections() {
			for _, j := range v.KeyStrings() {
				sl := strings.Split(j, " ")
				// 把 includedir 中的内容合到 instancecfgs 中，便于处理
				if sl[0] == "!includedir" {
					instancecfgs.Section(v.Name()).DeleteKey(j)
					files, _ := ListDir(sl[len(sl)-1], "cnf")
					for _, file := range files {
						cfg, _ := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true, UnescapeValueDoubleQuotes: true}, file)
						for _, v := range cfg.Sections() {
							if _, err := instancecfgs.NewSection(v.Name()); err == nil {
								for a, b := range v.KeysHash() {
									instancecfgs.Section(v.Name()).NewKey(a, b)
								}
							}
						}
					}
				}
				// TODO: 处理 include 单个文件
				if sl[0] == "!include" {
					// fmt.Println(j)
					// sl := strings.Split(j, " ")
					// files, _ := ListDir(sl[len(sl)-1], "cnf")
					// for _, file := range files {
					// 	cfg, _ := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, file)
					// 	for _, v := range cfg.Sections() {
					// 		_ = v
					// 	}
					// }
				}
			}
		}
	}

	sections = append(sections, instancecfgs.Sections()...)

	instances := make([]string, 0, 4)
	instancereg := regexp.MustCompile(`(mysqld)([0-9])([0-9]+)?$`)
	for _, v := range sections {
		if instancereg.MatchString(v.Name()) {
			instances = append(instances, v.Name())
		}
	}

	var m = map[string]MysqlIns{}

	for _, v := range instances {
		var (
			section      *ini.Section
			socket, port *ini.Key
		)

		if instancecfgs != nil {
			section, err = instancecfgs.GetSection(v)
			if err != nil {
				return err
			}
			socket, err = section.GetKey("socket")
			port, err = section.GetKey("port")
		}

		// 读取实例的配置（mon.cfg）中为每实例配置的参数，如果没有，则用默认配置
		var mycfg = MysqlIns{Flag: "host"}
		mysqlsection, err := c.GetSection(v)
		if err == nil {
			mycfg.User = mysqlsection.Key("user").String()
			mycfg.Pass = mysqlsection.Key("password").String()
			mycfg.Host = mysqlsection.Key("host").String()
			mycfg.Socket = mysqlsection.Key("socket").String()
			mycfg.Port, err = mysqlsection.Key("port").Int()
			if mycfg.Socket != "" {
				mycfg.Flag = "socket"
			}
		} else {
			mycfg = conf.MyDef
			mycfg.Socket = socket.MustString("/tmp/mysql.sock")
			mycfg.Port = port.MustInt(3306)
			mycfg.Flag = "socket"
		}
		// fmt.Println(mycfg)

		m[v] = mycfg
		// fmt.Println(conf)
	}
	conf.MysqlIns = m
	return nil
}

func timeout() {
	time.AfterFunc(TIME_OUT*time.Second, func() {
		log.Error("Execute timeout")
		os.Exit(1)
	})
}

func MysqlAlive(m *MysqlIns, ok bool) {
	data := NewMetric("mysql_alive_local", m)
	if ok {
		data.SetValue(1)
	}
	msg, err := sendData([]*MetaData{data})
	if err != nil {
		log.Errorf("Send alive data failed: %v", err)
		return
	}
	log.Infof("Alive data response %s: %s", m.String(), string(msg))
}

func FetchData(m *MysqlIns) (err error) {
	defer func() {
		MysqlAlive(m, err == nil)
	}()
	var db mysql.Conn
	// db := mysql.New("tcp", "", fmt.Sprintf("%s:%d", m.Host, m.Port),
	// 	cfg.User, cfg.Pass)
	if m.Flag == "host" {
		db = mysql.New("tcp", "", fmt.Sprintf("%s:%d", m.Host, m.Port), m.User, m.Pass)
	} else {
		db = mysql.New("unix", "", m.Socket, m.User, m.Pass)
	}
	db.SetTimeout(500 * time.Millisecond)
	if err = db.Connect(); err != nil {
		return
	}
	defer db.Close()

	data := make([]*MetaData, 0)
	// 自定义数据的结果集
	customData := make([]*MetaData, 0)

	globalStatus, err := GlobalStatus(m, db)
	if err != nil {
		return
	}
	data = append(data, globalStatus...)

	globalVars, err := GlobalVariables(m, db)
	if err != nil {
		return
	}
	data = append(data, globalVars...)

	innodbState, err := innodbStatus(m, db)
	if err != nil {
		return
	}
	data = append(data, innodbState...)

	slaveState, err := slaveStatus(m, db)
	if err != nil {
		return
	}
	data = append(data, slaveState...)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 搜集 processlist
		processState, err := processList(m, db)
		if err != nil {
			log.Println(err)
			return
		}

		customData = append(customData, processState...)
		// 发送自定义的数据
		err = sendCustomData(customData)
		if err != nil {
			log.Error("sendCustomData Error:", err)
			return
		}
		log.Infof("sendCustomData success")
	}()

	msg, err := sendData(data)
	if err != nil {
		return
	}
	log.Infof("Send response %s: %s", m.String(), string(msg))
	wg.Wait()
	return
}

func (m *MysqlIns) String() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}

func main() {
	log.Info("MySQL Monitor for falcon")
	go timeout()
	var wg sync.WaitGroup
	for key, mysqlins := range cfg.MysqlIns {

		ins := MysqlIns{}
		ins = mysqlins
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			err := FetchData(&ins)
			if err != nil {
				log.Error(key, err)
			}
		}(&wg)
	}
	wg.Wait()
}
