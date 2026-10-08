package stream

// Phase describes bootstrap / handoff operational state.
type Phase string

const (
	PhaseHistorical Phase = "historical"
	PhaseHandoff    Phase = "handoff"
	PhaseLive       Phase = "live"
	PhaseWaiting    Phase = "waiting"
	PhaseFailed     Phase = "failed"
)

// PhaseMetricValue maps phases to Prometheus gauge values.
func PhaseMetricValue(p Phase) float64 {
	switch p {
	case PhaseHistorical:
		return 0
	case PhaseHandoff:
		return 1
	case PhaseLive:
		return 2
	case PhaseWaiting:
		return 3
	case PhaseFailed:
		return 4
	default:
		return -1
	}
}
