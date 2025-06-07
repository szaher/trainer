package v1alpha1

// RayMLPolicySourceApplyConfiguration represents a declarative configuration of the RayMLPolicySource type for use
// with apply.
type RayMLPolicySourceApplyConfiguration struct {
	NumWorkers *int32 `json:"numWorkers,omitempty"`
}

// RayMLPolicySource constructs a declarative configuration of the RayMLPolicySource type for use with apply.
func RayMLPolicySource() *RayMLPolicySourceApplyConfiguration {
	return &RayMLPolicySourceApplyConfiguration{}
}

// WithNumWorkers sets the NumWorkers field in the declarative configuration to the given value
// and returns the receiver.
func (b *RayMLPolicySourceApplyConfiguration) WithNumWorkers(value int32) *RayMLPolicySourceApplyConfiguration {
	b.NumWorkers = &value
	return b
}
