package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Ayomide1x/object-store/internal/upload"
)

// errorBody is every error response: a message for people and a code for
// programs (the gRPC code, or the gateway's own reason).
type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, reason, msg string) {
	writeJSON(w, code, errorBody{Error: msg, Code: reason})
}

// httpStatus maps a failure of the upstream work behind a request (a
// coordinator call, or the replica reads and writes) to an HTTP status and
// whether the client may retry (a Retry-After header).
//
//	INVALID_ARGUMENT     400  the request is wrong; retrying won't help
//	NOT_FOUND            404  the key does not resolve
//	FAILED_PRECONDITION  503  commit refused (fewer than 2 verified replicas,
//	                          or the upload was abandoned): a cluster condition
//	UNAVAILABLE          503  too few nodes alive, or the coordinator unreachable
//	RESOURCE_EXHAUSTED   503  busy
//	quorum not reached   503  a chunk got fewer than 2 replica acknowledgements
//	DATA_LOSS            500  a chunk has no healthy replica: the data is lost
//	DEADLINE_EXCEEDED    504  the coordinator did not answer in time
//	anything else        502  upstream failed in a way we don't classify,
//	                          including every replica of a chunk failing a read
func httpStatus(err error) (code int, retry bool, reason string) {
	if errors.Is(err, upload.ErrQuorum) {
		return http.StatusServiceUnavailable, true, "QUORUM_NOT_REACHED"
	}
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusBadGateway, false, "UPSTREAM_FAILED"
	}
	reason = st.Code().String()
	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, false, "INVALID_ARGUMENT"
	case codes.NotFound:
		return http.StatusNotFound, false, "NOT_FOUND"
	case codes.FailedPrecondition:
		return http.StatusServiceUnavailable, true, "FAILED_PRECONDITION"
	case codes.Unavailable:
		return http.StatusServiceUnavailable, true, "UNAVAILABLE"
	case codes.ResourceExhausted:
		return http.StatusServiceUnavailable, true, "RESOURCE_EXHAUSTED"
	case codes.DataLoss:
		return http.StatusInternalServerError, false, "DATA_LOSS"
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, false, "DEADLINE_EXCEEDED"
	default:
		return http.StatusBadGateway, false, upperSnake(reason)
	}
}

// upperSnake turns grpc-go's code names ("Internal", "Unknown") into the
// wire spelling ("INTERNAL", "UNKNOWN").
func upperSnake(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' && i > 0 {
			out = append(out, '_')
		}
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out = append(out, c)
	}
	return string(out)
}

func writeUpstreamError(w http.ResponseWriter, err error) {
	code, retry, reason := httpStatus(err)
	if retry {
		w.Header().Set("Retry-After", "5")
	}
	writeError(w, code, reason, err.Error())
}
