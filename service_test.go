//go:build requires_docker
// +build requires_docker

package e2e

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitSumMetric(t *testing.T) {
	// Listen on a random port before starting the HTTP server, to
	// make sure the port is already open when we'll call WaitSumMetric()
	// the first time (this avoid flaky tests).
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	// Get the port.
	_, addrPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(addrPort)
	require.NoError(t, err)

	// Start an HTTP server exposing the metrics.
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`
# HELP metric_c cheescake
# TYPE metric_c gauge
metric_c 20
# HELP metric_a cheescake
# TYPE metric_a gauge
metric_a 1
metric_a{first="value1"} 10
metric_a{first="value1", something="x"} 4
metric_a{first="value1", something2="a"} 203
metric_a{first="value2"} 2
metric_a{second="value1"} 1
# HELP metric_b cheescake
# TYPE metric_b gauge
metric_b 1000
# HELP metric_b_counter cheescake
# TYPE metric_b_counter counter
metric_b_counter 1020
# HELP metric_b_hist cheescake
# TYPE metric_b_hist histogram
metric_b_hist_count 5
metric_b_hist_sum 124
metric_b_hist_bucket{le="5.36870912e+08"} 1
metric_b_hist_bucket{le="+Inf"} 5
# HELP metric_b_summary cheescake
# TYPE metric_b_summary summary
metric_b_summary_sum 22
metric_b_summary_count 1
`))
		}),
	}
	defer func() { _ = srv.Close() }()

	go func() {
		_ = srv.Serve(ln)
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: port,
			},
		},
	}

	s.SetBackoff(backoff.Config{
		MinBackoff: 300 * time.Millisecond,
		MaxBackoff: 600 * time.Millisecond,
		MaxRetries: 50,
	})
	require.NoError(t, s.WaitSumMetrics(Equals(221), "metric_a"))

	// No retry.
	s.SetBackoff(backoff.Config{
		MinBackoff: 0,
		MaxBackoff: 0,
		MaxRetries: 1,
	})
	require.Error(t, s.WaitSumMetrics(Equals(16), "metric_a"))

	require.NoError(t, s.WaitSumMetrics(Equals(1000), "metric_b"))
	require.NoError(t, s.WaitSumMetrics(Equals(1020), "metric_b_counter"))
	require.NoError(t, s.WaitSumMetrics(Equals(124), "metric_b_hist"))
	require.NoError(t, s.WaitSumMetrics(Equals(22), "metric_b_summary"))

	require.NoError(t, s.WaitSumMetrics(EqualsAmongTwo, "metric_a", "metric_a"))
	require.Error(t, s.WaitSumMetrics(EqualsAmongTwo, "metric_a", "metric_b"))

	require.NoError(t, s.WaitSumMetrics(GreaterAmongTwo, "metric_b", "metric_a"))
	require.Error(t, s.WaitSumMetrics(GreaterAmongTwo, "metric_a", "metric_b"))

	require.NoError(t, s.WaitSumMetrics(LessAmongTwo, "metric_a", "metric_b"))
	require.Error(t, s.WaitSumMetrics(LessAmongTwo, "metric_b", "metric_a"))

	require.Error(t, s.WaitSumMetrics(Equals(0), "non_existing_metric"))

	// A missing metric counts as a retry, so the wait stops when the retries run out.
	requireWaitGivesUp(t, func() error {
		return s.WaitSumMetricsWithOptions(Equals(0), []string{"non_existing_metric"}, WaitMissingMetrics)
	})

	c := NewCompositeHTTPService(s)
	c.retryBackoff = backoff.New(t.Context(), backoff.Config{MaxRetries: 1})
	requireWaitGivesUp(t, func() error {
		return c.WaitSumMetricsWithOptions(Equals(0), []string{"non_existing_metric"}, WaitMissingMetrics)
	})
}

// requireWaitGivesUp fails the test if wait does not return a missing metric error in time.
func requireWaitGivesUp(t *testing.T, wait func() error) {
	t.Helper()

	errs := make(chan error, 1)
	go func() { errs <- wait() }()

	select {
	case err := <-errs:
		require.ErrorContains(t, err, "unable to find metrics")
		require.ErrorContains(t, err, errMissingMetric.Error())
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not stop on a missing metric")
	}
}

func TestWaitSumMetric_Nan(t *testing.T) {
	// Listen on a random port before starting the HTTP server, to
	// make sure the port is already open when we'll call WaitSumMetric()
	// the first time (this avoid flaky tests).
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	// Get the port.
	_, addrPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(addrPort)
	require.NoError(t, err)

	// Start an HTTP server exposing the metrics.
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`
# HELP metric_c cheescake
# TYPE metric_c GAUGE
metric_c 20
# HELP metric_a cheescake
# TYPE metric_a GAUGE
metric_a 1
metric_a{first="value1"} 10
metric_a{first="value1", something="x"} 4
metric_a{first="value1", something2="a"} 203
metric_a{first="value1", something3="b"} Nan
metric_a{first="value2"} 2
metric_a{second="value1"} 1
# HELP metric_b cheescake
# TYPE metric_b GAUGE
metric_b 1000
`))
		}),
	}
	defer func() { _ = srv.Close() }()

	go func() {
		_ = srv.Serve(ln)
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: port,
			},
		},
	}

	s.SetBackoff(backoff.Config{
		MinBackoff: 300 * time.Millisecond,
		MaxBackoff: 600 * time.Millisecond,
		MaxRetries: 50,
	})
	require.NoError(t, s.WaitSumMetrics(Equals(math.NaN()), "metric_a"))
}

func TestWaitSumMetric_WaitMissingMetrics(t *testing.T) {
	// Listen on a random port before starting the HTTP server, to
	// make sure the port is already open when we'll call WaitSumMetric()
	// the first time (this avoid flaky tests).
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	// Get the port.
	_, addrPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	port, err := strconv.Atoi(addrPort)
	require.NoError(t, err)

	// Start an HTTP server that exposes metric_b from the third request.
	var requests atomic.Int32
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			metrics := "# TYPE metric_a gauge\nmetric_a 1\n"
			if requests.Add(1) >= 3 {
				metrics += "# TYPE metric_b gauge\nmetric_b 2\n"
			}
			_, _ = w.Write([]byte(metrics))
		}),
	}
	defer func() { _ = srv.Close() }()

	go func() {
		_ = srv.Serve(ln)
	}()

	s := &HTTPService{
		httpPort: 0,
		ConcreteService: &ConcreteService{
			networkPortsContainerToLocal: map[int]int{
				0: port,
			},
		},
	}

	cfg := backoff.Config{MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond, MaxRetries: 5}
	s.SetBackoff(cfg)
	require.NoError(t, s.WaitSumMetricsWithOptions(Equals(2), []string{"metric_b"}, WaitMissingMetrics))
	require.Equal(t, int32(3), requests.Load())

	// CompositeHTTPService has its own copy of the wait loop.
	requests.Store(0)
	c := NewCompositeHTTPService(s)
	c.retryBackoff = backoff.New(t.Context(), cfg)
	require.NoError(t, c.WaitSumMetricsWithOptions(Equals(2), []string{"metric_b"}, WaitMissingMetrics))
	require.Equal(t, int32(3), requests.Load())
}

func TestParseDockerPort(t *testing.T) {
	_, err := parseDockerIPv4Port("")
	assert.Error(t, err)

	actual, err := parseDockerIPv4Port("0.0.0.0:36999")
	assert.NoError(t, err)
	assert.Equal(t, 36999, actual)

	actual, err = parseDockerIPv4Port("0.0.0.0:49155\n:::49156")
	assert.NoError(t, err)
	assert.Equal(t, 49155, actual)

	actual, err = parseDockerIPv4Port(":::49156\n0.0.0.0:49155")
	assert.NoError(t, err)
	assert.Equal(t, 49155, actual)
}
