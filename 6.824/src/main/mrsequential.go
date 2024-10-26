package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
// 定义一个新的类型，名为 ByKey
// ByKey 是一个切片，切片中的元素是 mr.KeyValue 类型的值。
type ByKey []mr.KeyValue

// for sorting by key.
// 这些是 sort.Interface 接口中的三个方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	// 判断参数是否达到 3，参数至少为 3
	// 可以有多个输入文本作为 corpus
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// 加载动态库 wc.go 中的函数	
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	// 在 Go 语言中定义一个空的切片（slice），该切片的元素类型为 mr.KeyValue。具体解释如下：
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		// 根据文件路径打开文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// 读取文件内容
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// 把文件名和文件的内容作为参数送给动态库中的 Map 函数
		kva := mapf(filename, string(content))
		// 把 Map 函数的返回值放进 intermediate 的末尾
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	// 创建一个文件，名为 mr-out-0
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 跳过所有相同的 intermediate (之前排序过)
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// 把从 i 到 j 所有的 intermediate 的 Value 放进 values
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 对这些 values 使用 Reduce 函数
		output := reducef(intermediate[i].Key, values)

		// 把 reduce 的结果放进 输出文件中
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
// filename: 动态库地址
// 返回值是两个类型为 func(string, string) []mr.KeyValue 和 func(string, []string) string 的函数
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 打开动态库
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	// 寻找名为 Map 的函数
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	// 寻找名 Reduce 的函数
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	// 返回这两个函数指针
	return mapf, reducef
}
