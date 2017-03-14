package utils

import (
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	v1beta "k8s.io/client-go/pkg/apis/apps/v1beta1"
	"encoding/json"
)

type KubeClient struct {
	cli *kubernetes.Clientset
}

func NewKubeClient(url, configPath string) (*KubeClient, error) {
	config, err := clientcmd.BuildConfigFromFlags(url, configPath)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &KubeClient{cli:clientset}, nil
}

func (k *KubeClient) CreateNamespace(jsonReq []byte) (*v1.Namespace, error) {
	nspace := &v1.Namespace{}
	err := json.Unmarshal(jsonReq, nspace)
	if err != nil {
		return nil, err
	}

	return k.cli.Namespaces().Create(nspace)
}

func (k *KubeClient) ListNamespace(queryReq []byte) (*v1.NamespaceList, error) {
	options := &v1.ListOptions{}
	err := json.Unmarshal(queryReq, options)
	if err != nil {
		return nil, err
	}

	return k.cli.Namespaces().List(*options)
}

func (k *KubeClient) CreateDeployment(namespace string, jsonReq []byte) (*v1beta1.Deployment, error) {
	deploy := &v1beta1.Deployment{
	}
	err := json.Unmarshal(jsonReq, deploy)
	if err != nil {
		return nil, err
	}

	return k.cli.Deployments(namespace).Create(deploy)
}

func (k *KubeClient) ListDeployment(namespace string, queryReq []byte) (*v1beta1.DeploymentList, error) {
	options := &v1.ListOptions{}
	err := json.Unmarshal(queryReq, options)
	if err != nil {
		return nil, err
	}

	return k.cli.Deployments(namespace).List(*options)
}

func (k *KubeClient) CreateStatefulSet(namespace string, jsonReq []byte) (*v1beta.StatefulSet, error) {
	sSet := &v1beta.StatefulSet{}
	err := json.Unmarshal(jsonReq, sSet)
	if err != nil {
		return nil, err
	}

	return k.cli.StatefulSets(namespace).Create(sSet)
}

func (k *KubeClient) ListStatefulSet(namespace string, queryReq []byte) (*v1beta.StatefulSet, error) {
	options := &v1beta.StatefulSet{}
	err := json.Unmarshal(queryReq, options)
	if err != nil {
		return nil, err
	}

	return k.cli.StatefulSets(namespace).Create(options)
}

func (k *KubeClient) CreatePod(namespace string, jsonReq []byte) (*v1.Pod, error) {
	pod := &v1.Pod{}
	err := json.Unmarshal(jsonReq, pod)
	if err != nil {
		return nil, err
	}

	return k.cli.Pods(namespace).Create(pod)
}

func (k *KubeClient) ListPod(namespace string, queryReq []byte) (*v1.PodList, error) {
	options := &v1.ListOptions{}
	err := json.Unmarshal(queryReq, options)
	if err != nil {
		return nil, err
	}

	return k.cli.Pods(namespace).List(*options)
}

func (k *KubeClient) CreateService(namespace string, jsonReq []byte) (*v1.Service, error) {
	service := &v1.Service{}
	err := json.Unmarshal(jsonReq, service)
	if err != nil {
		return nil, err
	}
	return k.cli.Services(namespace).Create(service)
}

func (k *KubeClient) ListService(namespace string, queryReq []byte) (*v1.ServiceList, error) {
	options := &v1.ListOptions{}
	err := json.Unmarshal(queryReq, options)
	if err != nil {
		return nil, err
	}

	return k.cli.Services(namespace).List(*options)
}


func (k *KubeClient) CreateNode(jsonReq []byte) (*v1.Node, error) {
	node := &v1.Node{}
	err := json.Unmarshal(jsonReq, node)
	if err != nil {
		return nil, err
	}
	return k.cli.Nodes().Create(node)
}

func (k *KubeClient) ListNode(queryReq []byte) (*v1.NodeList, error) {
	options := &v1.ListOptions{}
	err := json.Unmarshal(queryReq, options)
	if err != nil {
		return nil, err
	}

	return k.cli.Nodes().List(*options)
}

