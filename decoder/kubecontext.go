package decoder
import (
    "context"
    "fmt"
	"log"
	"os"
    "github.com/cloudflare/ebpf_exporter/config"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
)
const (
    // when fail get kubernetes labe fail use DefaultAirflowContextValue  as default
    DefaultAirflowContextValue = "unknown"
)
type AirflowInfo struct {
    dagId  string
    taskId string
    runId  string
}
type AirflowContext struct {
    airflowContext map[string]AirflowInfo
}
type AirflowDagId struct {
    ctx AirflowContext
}
type AirflowTaskId struct {
    ctx AirflowContext
}
type AirflowRunId struct {
    ctx AirflowContext
}
func (i *AirflowDagId) Decode(in []byte, conf config.Decoder) ([]byte, error) {
    info := i.ctx.getAirflowInfo()
    b := []byte(info.dagId)
    return b, nil
}
func (i *AirflowTaskId) Decode(in []byte, conf config.Decoder) ([]byte, error) {
    info := i.ctx.getAirflowInfo()
    b := []byte(info.taskId)
    return b, nil
}
func (i *AirflowRunId) Decode(in []byte, conf config.Decoder) ([]byte, error) {
    info := i.ctx.getAirflowInfo()
    b := []byte(info.runId)
    return b, nil
}
func (a *AirflowContext) getAirflowInfo() (info AirflowInfo) {
    // TODO not using AirflowContext
    info.dagId = DefaultAirflowContextValue
    info.taskId = DefaultAirflowContextValue
    info.runId = DefaultAirflowContextValue
    config, err := rest.InClusterConfig()
    if err != nil {
        panic(err.Error())
    }
    // creates the clientset
    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        panic(err.Error())
    }

	// get hostname from local machine
	hostname, err := os.Hostname()
	localpods, err := clientset.CoreV1().Pods("kube-system").List(context.TODO(), metav1.ListOptions{
		FieldSelector: "metadata.name=" + hostname,
	})
	if err != nil {
		log.Printf("Failed to get pods with hostname as name: %v", err)
		panic(err.Error())
	}
	if len(localpods.Items) != 1 {
		log.Printf("Error: more than one pod with hostname as name")
		panic(err.Error())
	}
	pod := localpods.Items[0]
	nodeName := pod.Spec.NodeName

	log.Printf("nodeName: %s", nodeName)
	

	// get pods in all the namespaces by omitting namespace
	// Or specify namespace to get pods in particular namespace
	pods, err := clientset.CoreV1().Pods("airflow").List(context.TODO(), metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))
	for _, pod := range pods.Items {
		fmt.Printf("Found %s:\n", pod.Name)
		for k, v := range pod.Labels {
			if k == "dag_id" {
				info.dagId = v
			}
			if k == "task_id" {
				info.taskId = v
			}
			if k == "run_id" {
				info.runId = v
			}
		}
		fmt.Printf("%s: dag_id \"%s\", task_id: \"%s\", run_id: \"%s\"\n", pod.Name, info.dagId, info.taskId, info.runId)
	}
    
	return
}