package ray

import (
	"context"
	"strings"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	trainer "github.com/kubeflow/trainer/pkg/apis/trainer/v1alpha1"
	"github.com/kubeflow/trainer/pkg/runtime"
	"github.com/kubeflow/trainer/pkg/runtime/framework"
)

// Ray implements a simple component builder plugin that creates a RayCluster and RayJob.
type Ray struct{}

var _ framework.ComponentBuilderPlugin = (*Ray)(nil)

const Name = "Ray"

func New(context.Context, client.Client, client.FieldIndexer) (framework.Plugin, error) {
	return &Ray{}, nil
}

func (r *Ray) Name() string { return Name }

func (r *Ray) Build(ctx context.Context, info *runtime.Info, trainJob *trainer.TrainJob) ([]any, error) {
	if info == nil || trainJob == nil || info.RuntimePolicy.MLPolicySource == nil || info.RuntimePolicy.MLPolicySource.Ray == nil {
		return nil, nil
	}

	var workers int32
	if info.RuntimePolicy.MLPolicySource.Ray.NumWorkers != nil {
		workers = *info.RuntimePolicy.MLPolicySource.Ray.NumWorkers
	}

	clusterName := trainJob.Name + "-ray"
	cluster := &rayv1.RayCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rayv1.GroupVersion.String(),
			Kind:       "RayCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: trainJob.Namespace,
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "ray-head",
							Image: "rayproject/ray:latest",
						}},
					},
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
				GroupName: "ray-worker",
				Replicas:  &workers,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "ray-worker",
							Image: "rayproject/ray:latest",
						}},
					},
				},
			}},
		},
	}

	entrypoint := strings.Join(trainJob.Spec.Trainer.Command, " ")
	if len(trainJob.Spec.Trainer.Args) > 0 {
		entrypoint = entrypoint + " " + strings.Join(trainJob.Spec.Trainer.Args, " ")
	}

	job := &rayv1.RayJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rayv1.GroupVersion.String(),
			Kind:       "RayJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      trainJob.Name,
			Namespace: trainJob.Namespace,
		},
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &cluster.Spec,
			Entrypoint:     entrypoint,
		},
	}

	return []any{cluster, job}, nil
}
