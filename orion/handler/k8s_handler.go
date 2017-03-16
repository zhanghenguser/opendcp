package handler

import (
	. "weibo.com/opendcp/orion/models"
	"github.com/astaxie/beego"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/pkg/api/v1"
	"errors"
	"time"
	"encoding/json"
	"weibo.com/opendcp/orion/models"
)

const (
	TIME_WAIT = 10
	MAX_TRY   = 20

	FRESH_SD    = "fresh_sd"
	APPEND_NODE = "append_nodes"
)

type KubeHandler struct {
	clientSet *kubernetes.Clientset
}

func (k *KubeHandler) Init() error {
	//config, err := clientcmd.BuildConfigFromFlags("10.86.203.97", "./conf/k8s_config")
	config, err := clientcmd.BuildConfigFromFlags(beego.AppConfig.String("k8s_master"), beego.AppConfig.String("k8s_config_path"))
	if err != nil {
		return err
	}
	k.clientSet, err = kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	return nil
}

func (k *KubeHandler) ListAction() []ActionImpl {
	return []models.ActionImpl{
		{
			Name: FRESH_SD,
			Desc: "refresh kubernetes service discovery",
			Type: "k8s",
			Params: map[string]interface{}{
				"service_spec":      "String",
				"create":            "Boolean",
			},
		},
		{
			Name: APPEND_NODE,
			Desc: "append node to pool",
			Type: "k8s",
			Params: map[string]interface{}{
				"pod_spec":      "String",
				"create":        "Boolean",
			},
		},
	}
}

func (k *KubeHandler) GetType() string {
	return "k8s"
}

func (k *KubeHandler) GetLog(nodeState *NodeState) string {
	return ""
}

func (k *KubeHandler) Handle(action *ActionImpl, actionParams map[string]interface{}, nodes []*NodeState, corrId string) *HandleResult {
	switch action.Name {
	case FRESH_SD:
		{
			serviceSpec := ""
			if str_obj, ok := actionParams["service_spec"]; !ok {
				return Err("need param service_spec")
			} else {
				serviceSpec = str_obj.(string)
			}
			isCreate := true
			if str_obj, ok := actionParams["create"]; ok {
				isCreate = str_obj.(bool)
			}

			err := k.FreshService(nodes, serviceSpec, isCreate)
			if err != nil {
				beego.Error(err)
				return Err(err.Error())
			}

			return &HandleResult{
				Code:   CODE_SUCCESS,
				Result: nil,
			}
		}
	case APPEND_NODE:
		{
			podSpec := ""
			if str_obj, ok := actionParams["pod_spec"]; !ok {
				return Err("need param pod_spec")
			} else {
				podSpec = str_obj.(string)
			}
			isCreate := true
			if str_obj, ok := actionParams["create"]; ok {
				isCreate = str_obj.(bool)
			}

			err := k.AppendNodes(nodes, podSpec, isCreate)
			if err != nil {
				beego.Error(err)
				return Err(err.Error())
			}

			return &HandleResult{
				Code:   CODE_SUCCESS,
				Result: nil,
			}
		}
	default:
		{
			return Err("command not supoorted")
		}
	}

	return &HandleResult{
		Code:   CODE_SUCCESS,
		Msg:    "OK",
		Result: nil,
	}
}

//@todo: not complete
func (k *KubeHandler) install_node(nodes []*NodeState) {

}

// Expose
func (k *KubeHandler) FreshService(nodes []*NodeState, serviceSpec string, isCreate bool) error {
	poolName := nodes[0].Pool.Name
	serviceName := nodes[0].Pool.Service.Name
	clusterName := nodes[0].Pool.Service.Cluster.Name

	svcSpec := v1.ServiceSpec{}
	err := json.Unmarshal([]byte(serviceSpec), &svcSpec)
	if err != nil {
		beego.Error("json dencode template failed, err: ", err.Error())
		return err
	}

	err = k.checkNamespace(clusterName)
	if err != nil {
		return err
	}

	service := k.getService(clusterName, serviceName)
	if service == nil {
		if !isCreate {
			beego.Info("service not found and will not be create, poolName: ", poolName, ", serviceName: ", serviceName)
			return errors.New("service is not found")
		}

		beego.Info("service ", serviceName, "not found, now create")

		if err := k.createService(clusterName, serviceName, svcSpec); err != nil {
			return err
		}
	} else {
		beego.Info("service ", serviceName, "already exist, now update it")
		service.Spec = svcSpec
		k.updateService(clusterName, service)
	}

	return nil
}

// Expose
func (k *KubeHandler) AppendNodes(nodes []*NodeState, podSpec string, isCreate bool) error {
	if len(nodes) == 0 {
		return nil
	}

	spec := v1.PodSpec{}
	err := json.Unmarshal([]byte(podSpec), &spec)
	if err != nil {
		beego.Error("json dencode template failed, err: ", err.Error())
		return err
	}

	poolName := nodes[0].Pool.Name
	serviceName := nodes[0].Pool.Service.Name
	clusterName := nodes[0].Pool.Service.Cluster.Name

	err = k.checkNamespace(clusterName)
	if err != nil {
		return err
	}

	deploy := k.getDeployment(clusterName, poolName)
	if deploy == nil {
		if !isCreate {
			beego.Info("deployment not found and will not be create, poolName: ", poolName, ", serviceName: ", serviceName)
			return errors.New("deployment is not found")
		}

		if err := k.createDeployment(clusterName, serviceName, poolName, spec, len(nodes)); err != nil {
			return err
		}
	} else {
		oldVal := 0
		if deploy.Spec.Replicas != nil {
			oldVal = int(*deploy.Spec.Replicas)
		}
		newVal := int32(oldVal + len(nodes))
		deploy.Spec.Replicas = &newVal
		if err := k.updateDeployment(clusterName, deploy); err != nil {
			return err
		}
	}

	return nil
}

func (k *KubeHandler) updateDeployment(clusterName string, deploy *v1beta1.Deployment) error {
	_, err := k.clientSet.Deployments(clusterName).Update(deploy)
	if err != nil {
		beego.Error("update deployment failed, cluster: ", clusterName, ", err: ", err.Error())
		return err
	}
	return nil
}

func (k *KubeHandler) getDeployment(clusterName, poolName string) *v1beta1.Deployment {
	ret, err := k.clientSet.Deployments(clusterName).Get(poolName)
	if err != nil || ret == nil {
		return nil
	}
	return ret
}

// remove deployment
func (k *KubeHandler) removeDeployment(clusterName, poolName string) error {
	err := k.clientSet.Deployments(clusterName).Delete(poolName, nil)
	if err != nil {
		beego.Error("delete deployment failed, pool: ", poolName, " cluster: ", clusterName, ", err: ", err.Error())
		return err
	}
	beego.Info("delete deployment success, pool: ", poolName, " cluster: ", clusterName)
	return nil
}

func (k *KubeHandler) createDeployment(clusterName, serviceName, poolName string, spec v1.PodSpec, nodeCount int) error {
	deploy := &v1beta1.Deployment{
	}
	deploy.Name = poolName
	rep := int32(nodeCount)
	deploy.Spec.Replicas = &rep
	deploy.Spec.Template.Spec = spec
	deploy.Spec.Template.Labels = map[string]string{
		"app": serviceName,
		"pool":poolName,
	}

	if _, err := k.clientSet.Deployments(clusterName).Create(deploy); err != nil {
		return err
	}

	deploy_error := false
	ready_count := nodeCount
	//wait for ready
	for i := 0; i < MAX_TRY; i++ {
		deploy, err := k.clientSet.Deployments(clusterName).Get(poolName)
		if err != nil || deploy == nil {
			beego.Error("get deployment failed")
		}
		for _, con := range deploy.Status.Conditions {
			beego.Debug("deployment status is ", con.String())
			if con.Type == v1beta1.DeploymentReplicaFailure {
				// have one or more pod failed
				deploy_error = true
				beego.Error("deplyment status is failed, info:", con.Message)
				break
			}
			if con.Type == v1beta1.DeploymentAvailable {
				ready_count -= 1
			}
		}
		if ready_count == 0 {
			beego.Debug("deployment status is all green")
			//all success
			break
		}
		beego.Info("wait for deployment, try ", string(i), " times")
		time.Sleep(TIME_WAIT)
	}

	if ready_count != 0 || deploy_error {
		// remove deploy is have error
		k.removeDeployment(clusterName, poolName)
		return errors.New("deployment init faile..")
	}
	return nil
}

func (k *KubeHandler) getService(clusterName, serviceName string) *v1.Service {
	ret, err := k.clientSet.Services(clusterName).Get(serviceName)
	if err != nil || ret == nil {
		return nil
	}
	return ret
}

// check service and create
func (k *KubeHandler) createService(clusterName, serviceName string, serviceSpec v1.ServiceSpec) error {
	service := &v1.Service{}
	service.Name = serviceName
	service.Spec = serviceSpec

	service.Spec.Selector = map[string]string{
		"app":serviceName,
	}
	if _, err := k.clientSet.Services(clusterName).Create(service); err != nil {
		beego.Error("service ", serviceName, "create failed, err: ", err.Error())
		return err
	}
	return nil
}

func (k *KubeHandler) updateService(clusterName string, service *v1.Service) error {
	if _, err := k.clientSet.Services(clusterName).Update(service); err != nil {
		beego.Error("service ", service.Name, "update failed, err: ", err.Error())
		return err
	}
	return nil
}

func (k *KubeHandler) checkNamespace(clusterName string) error {
	_, err := k.clientSet.Namespaces().Get(clusterName)
	if err != nil {
		beego.Info("namespace ", clusterName, " not found, now create")
		newNs := &v1.Namespace{
		}
		newNs.Name = clusterName
		_, err = k.clientSet.Namespaces().Create(newNs)
		if err != nil {
			beego.Error("create namspace failed, cluster: ", clusterName, ", err: ", err.Error())
			return err
		}
		return nil
	}
	beego.Info("namspace ", clusterName, " already exist")
	return nil
}

// remove service if no use at last
func (k *KubeHandler) checkNoUseService(clusterName, serviceName string) error {
	list, err := k.clientSet.Deployments(clusterName).List(v1.ListOptions{})
	if err != nil {
		beego.Error("list deployment failed, err:", err.Error())
		return err
	}

	deployCount := 0
	for _, d := range list.Items {
		if val, ok := d.Labels["app"]; ok {
			if val == serviceName {
				deployCount += 1
			}
		}
	}
	if deployCount == 0 {
		beego.Info("service ", serviceName, " is not use, will be remove")
		err := k.clientSet.Services(clusterName).Delete(serviceName, nil)
		if err != nil {
			beego.Error("delete service ", serviceName, " failed, err: ", err.Error())
			return err
		}
		beego.Info("delete service ", serviceName, "success")
	} else {
		beego.Info("service ", serviceName, " is still in use, not remove")
	}
	return nil
}
