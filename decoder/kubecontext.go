package decoder

import (
	"context"
	"fmt"
	"log"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"github.com/iovisor/gobpf/bcc"

	"github.com/cloudflare/ebpf_exporter/config"
)

const (
	// when fail get kubernetes labe fail use DefaultKubeContextValue  as default
	DefaultKubeContextValue = "unknown"
)

// KubeInfo kubernetes context info
type KubeInfo struct {
	kubePodNamespace  string
	kubePodName       string
	kubeContainerName string
}

// KubeContext kubernetes context info cache
type KubeContext struct {
	kubeContext map[string]KubeInfo
}

// KubePodNamespace is a decoder that transforms pid representation into kubernetes pod namespace
type KubePodNamespace struct {
	ctx KubeContext
}

// KubePodName is a decoder that transforms pid representation into kubernetes pod name
type KubePodName struct {
	ctx KubeContext
}

// KubeContainerName is a decoder that transforms pid representation into kubernetes pod container name
type KubeContainerName struct {
	ctx KubeContext
}

// KubeContainerNameOrPid is a decoder that transforms pid representation into kubernetes pod container name or pid
type KubeContainerNameOrPid struct {
	ctx KubeContext
}

// Decode transforms pid representation into a kubernetes namespace and pod as string
func (k *KubePodNamespace) Decode(in []byte, conf config.Decoder) ([]byte, error) {
	byteOrder := bcc.GetHostByteOrder()
	info := k.ctx.getKubeInfo(byteOrder.Uint32(in))
	b := []byte(info.kubePodNamespace)
	return b, nil
}

// Decode transforms pid representation into a kubernetes pod name as string
func (k *KubePodName) Decode(in []byte, conf config.Decoder) ([]byte, error) {
	byteOrder := bcc.GetHostByteOrder()
	info := k.ctx.getKubeInfo(byteOrder.Uint32(in))
	b := []byte(info.kubePodName)
	return b, nil
}

// Decode transforms pid representation into a kubernetes container name as string
func (k *KubeContainerName) Decode(in []byte, conf config.Decoder) ([]byte, error) {
	byteOrder := bcc.GetHostByteOrder()
	info := k.ctx.getKubeInfo(byteOrder.Uint32(in))
	b := []byte(info.kubeContainerName)
	return b, nil
}

// Decode transforms pid representation into a kubernetes container name, if no foud return pid instead
func (k *KubeContainerNameOrPid) Decode(in []byte, conf config.Decoder) ([]byte, error) {
	byteOrder := bcc.GetHostByteOrder()
	info := k.ctx.getKubeInfo(byteOrder.Uint32(in))
	if info.kubeContainerName == DefaultKubeContextValue {
		info.kubeContainerName = fmt.Sprintf("pid-%d", byteOrder.Uint32(in))
	}
	b := []byte(info.kubeContainerName)
	return b, nil
}

// getKubeInfo implement main logic convert container id to kubernetes context
func (k *KubeContext) getKubeInfo(pid uint32) (info KubeInfo) {
	info.kubePodNamespace = DefaultKubeContextValue
	info.kubePodName = DefaultKubeContextValue
	info.kubeContainerName = DefaultKubeContextValue


	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("error creating in-cluster config: %v", err)
		return
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Printf("error creating clientset: %v", err)
		return
	}

	// get pods in all the namespaces by omitting namespace
	// Or specify namespace to get pods in particular namespace
	pods, err := clientset.CoreV1().Pods("airflow").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Printf("error getting pods: %v", err)
		return
	}
	log.Printf("There are %d pods in the cluster\n", len(pods.Items))


	for _, pod := range pods.Items {
		isFound := false
		fmt.Printf("Labels for pod %s:\n", pod.Name)
		for k, v := range pod.Labels {
			fmt.Printf("   key: %s, value: %s\n", k, v)
			if k == "dag_id" {
				info.kubeContainerName = v
				isFound = true
			}
			if k == "task_id" {
				info.kubePodName = v
				isFound = true
			}
			if k == "run_id" {
				info.kubePodNamespace = v
				isFound = true
			}
		}
		if isFound == true {
			return
		}
		 
	}

	// Examples for error handling:
	// - Use helper functions e.g. errors.IsNotFound()
	// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
	_, err = clientset.CoreV1().Pods("airflow").Get(context.TODO(), "example-xxxxx", metav1.GetOptions{})
	if errors.IsNotFound(err) {
		log.Printf("Pod example-xxxxx not found in default namespace\n")
	} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
		log.Printf("Error getting pod %v\n", statusError.ErrStatus.Message)
	} else if err != nil {
		panic(err.Error())
	} else {
		log.Printf("Found example-xxxxx pod in default namespace\n")
	}

	time.Sleep(10 * time.Second)

	return
	
}