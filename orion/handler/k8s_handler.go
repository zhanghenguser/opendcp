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
)

const (
	TIME_WAIT = 10
	MAX_TRY   = 20
)

type KubeHandler struct {
	clientSet *kubernetes.Clientset
	actions   []ActionImpl
}

func (k *KubeHandler) Init() error {
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
	return k.actions
}

func (k *KubeHandler) GetType() string {
	return "k8s"
}

func (k *KubeHandler) GetLog(nodeState *NodeState) string {
	return ""
}

func (k *KubeHandler) Handle(action *ActionImpl, actionParams map[string]interface{}, nodes []*NodeState, corrId string) *HandleResult {
	switch action.Name {
	case "deploy":
		{
			err := k.deploy(nodes)
			if err != nil {
				return &HandleResult{
					Code:   CODE_ERROR,
					Msg:    err.Error(),
					Result: nil,
				}
			}
		}
	default:
		{
			return &HandleResult{
				Code:   CODE_ERROR,
				Msg:    "command not supoorted",
				Result: nil,
			}
		}
	}

	return &HandleResult{
		Code:   CODE_SUCCESS,
		Msg:    "OK",
		Result: nil,
	}
}

func (k *KubeHandler) deploy(nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	poolName := nodes[0].Pool.Name
	serviceName := nodes[0].Pool.Service.Name
	image := nodes[0].Pool.Service.DockerImage
	clusterName := nodes[0].Pool.Service.Cluster.Name

	deploy, err := k.get_deployment(clusterName, poolName)
	if err != nil {
		return err
	}

	if deploy == nil {
		if err := k.create_deployment(clusterName, serviceName, poolName, image, len(nodes)); err != nil {
			return err
		}
	} else {
		oldVal := 0
		if deploy.Spec.Replicas != nil {
			oldVal = int(*deploy.Spec.Replicas)
		}
		newVal := int32(oldVal + len(nodes))
		deploy.Spec.Replicas = &newVal
		if err := k.update_deployment(clusterName, deploy); err != nil {
			return err
		}
	}

	if err := k.checkService(clusterName, serviceName); err != nil {
		return err
	}

	return nil
}

//@todo: not complete
func (k *KubeHandler) install_node(nodes []*NodeState) {

}

func (k *KubeHandler) update_deployment(clusterName string, deploy *v1beta1.Deployment) error {
	_, err := k.clientSet.Deployments(clusterName).Update(deploy)
	if err != nil {
		beego.Error("update deployment failed, cluster: ", clusterName, ", err: ", err.Error())
		return err
	}
	return nil
}

func (k *KubeHandler) get_deployment(clusterName, poolName string) (*v1beta1.Deployment, error) {
	ret, err := k.clientSet.Deployments(clusterName).Get(poolName)
	if err != nil {
		beego.Error("get deployment failed, pool: ", poolName, " cluster: ", clusterName, ", err: ", err.Error())
		return nil, err
	}
	return ret, nil
}

// remove deployment
func (k *KubeHandler) remove_deployment(clusterName, poolName string) error {
	err := k.clientSet.Deployments(clusterName).Delete(poolName, nil)
	if err != nil {
		beego.Error("delete deployment failed, pool: ", poolName, " cluster: ", clusterName, ", err: ", err.Error())
		return err
	}
	beego.Info("delete deployment success, pool: ", poolName, " cluster: ", clusterName)
	return nil
}

func (k *KubeHandler) create_deployment(clusterName, serviceName, poolName, image string, nodeCount int) error {
	deploy := &v1beta1.Deployment{
	}
	deploy.Name = poolName
	rep := int32(nodeCount)
	deploy.Spec.Replicas = &rep
	deploy.Spec.Template.Spec.Containers = []v1.Container{
		{
			Name: serviceName,
			Image:image,
		},
	}
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
		k.remove_deployment(clusterName, poolName)
		return errors.New("deployment init faile..")
	}
	return nil
}

// check service and create
func (k *KubeHandler) checkService(clusterName, serviceName string) error {
	ret, err := k.clientSet.Services(clusterName).Get(serviceName)
	if err != nil {
		beego.Error("get service ", serviceName, "failed, err: ", err.Error())
		return err
	}
	if ret != nil {
		beego.Info("service ", serviceName, "already exist")
		return nil
	}

	beego.Info("service ", serviceName, "not found, now create")

	service := &v1.Service{}
	service.Name = serviceName
	service.Spec.Selector = map[string]string{
		"app":serviceName,
	}
	if _, err := k.clientSet.Services(clusterName).Create(service); err != nil {
		beego.Error("service ", serviceName, "create failed, err: ", err.Error())
		return err
	}
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
