package handler

import (
	. "weibo.com/opendcp/orion/models"
	"weibo.com/opendcp/orion/utils"
	"github.com/astaxie/beego"
)

var (
	kubeClient *utils.KubeClient
)

type K8sHandler struct {
	actions []ActionImpl
}

func (v *K8sHandler) Init() error {
	cli, err := utils.NewKubeClient(beego.AppConfig.String("k8s_master"), beego.AppConfig.String("k8s_config_path"))
	if err != nil {
		return err
	}
	kubeClient = cli
	return nil
}

func (v *K8sHandler) ListAction() []ActionImpl {
	return v.actions
}

func (v *K8sHandler) GetType() string {
	return "k8s"
}

func (v *K8sHandler) GetLog(nodeState *NodeState) string {
	return ""
}

func (v *K8sHandler) Handle(action *ActionImpl, actionParams map[string]interface{},
	nodes []*NodeState, corrId string) *HandleResult {

	return &HandleResult{
		Code:   CODE_SUCCESS,
		Msg:    "OK",
		Result: nil,
	}
}
