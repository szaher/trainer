package ray

import (
	"context"
	"fmt"
	"strings"

	batchv1ac "k8s.io/client-go/applyconfigurations/batch/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2ac "sigs.k8s.io/jobset/client-go/applyconfiguration/jobset/v1alpha2"

	trainer "github.com/kubeflow/trainer/pkg/apis/trainer/v1alpha1"
	"github.com/kubeflow/trainer/pkg/runtime"
	"github.com/kubeflow/trainer/pkg/runtime/framework"
)

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

	workers := int32(1)
	if info.RuntimePolicy.MLPolicySource.Ray.NumWorkers != nil {
		workers = *info.RuntimePolicy.MLPolicySource.Ray.NumWorkers
	}

	headJob := jobsetv1alpha2ac.ReplicatedJob().
		WithName("head").
		WithReplicas(1).
		WithTemplate(batchv1ac.JobTemplateSpec().
			WithSpec(batchv1ac.JobSpec().
				WithTemplate(corev1ac.PodTemplateSpec().
					WithSpec(corev1ac.PodSpec().
						WithRestartPolicy("OnFailure").
						WithContainers(
							corev1ac.Container().
								WithName("ray-head").
								WithImage("rayproject/ray:latest").
								WithCommand("/bin/bash", "-c").
								WithArgs("ray start --head --port=6379 --dashboard-host=0.0.0.0 && tail -f /dev/null"),
						),
					),
				),
			),
		)

	workerJob := jobsetv1alpha2ac.ReplicatedJob().
		WithName("worker").
		WithReplicas(workers).
		WithTemplate(batchv1ac.JobTemplateSpec().
			WithSpec(batchv1ac.JobSpec().
				WithTemplate(corev1ac.PodTemplateSpec().
					WithSpec(corev1ac.PodSpec().
						WithRestartPolicy("OnFailure").
						WithContainers(
							corev1ac.Container().
								WithName("ray-worker").
								WithImage("rayproject/ray:latest").
								WithCommand("/bin/bash", "-c").
								WithArgs("ray start --address=head-0:6379 --block"),
						),
					),
				),
			),
		)

	jobSet := jobsetv1alpha2ac.JobSet(trainJob.Name, trainJob.Namespace).
		WithSpec(jobsetv1alpha2ac.JobSetSpec().
			WithNetwork(jobsetv1alpha2ac.Network().WithPublishNotReadyAddresses(true)).
			WithReplicatedJobs(headJob, workerJob),
		).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(trainer.GroupVersion.String()).
			WithKind(trainer.TrainJobKind).
			WithName(trainJob.Name).
			WithUID(trainJob.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))

	entrypoint := strings.Join(trainJob.Spec.Trainer.Command, " ")
	if len(trainJob.Spec.Trainer.Args) > 0 {
		entrypoint = entrypoint + " " + strings.Join(trainJob.Spec.Trainer.Args, " ")
	}

	submit := batchv1ac.Job(trainJob.Name+"-ray-submit", trainJob.Namespace).
		WithSpec(batchv1ac.JobSpec().
			WithTemplate(corev1ac.PodTemplateSpec().
				WithSpec(corev1ac.PodSpec().
					WithRestartPolicy("OnFailure").
					WithContainers(corev1ac.Container().
						WithName("submit").
						WithImage("rayproject/ray:latest").
						WithCommand("/bin/bash", "-c").
						WithArgs(fmt.Sprintf("ray job submit --address http://head-0:8265 '%s'", entrypoint)),
					),
				),
			),
		).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(trainer.GroupVersion.String()).
			WithKind(trainer.TrainJobKind).
			WithName(trainJob.Name).
			WithUID(trainJob.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))

	return []any{jobSet, submit}, nil
}
