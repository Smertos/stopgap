package api

import (
	"encoding/json"
	"fmt"
	"unsafe"
)

var reactorState = struct {
	initialized bool
	allocations map[uint32][]byte
	response    []byte
}{
	allocations: map[uint32][]byte{},
}

//go:wasmexport stopgap_init
func StopgapInit() int32 {
	reactorState.initialized = true
	return 0
}

//go:wasmexport stopgap_malloc
func StopgapMalloc(length uint32) uint32 {
	if length == 0 {
		return 0
	}

	buf := make([]byte, int(length))
	ptr := bytesPointer(buf)
	reactorState.allocations[ptr] = buf
	return ptr
}

//go:wasmexport stopgap_free
func StopgapFree(ptr uint32, _ uint32) {
	if ptr == 0 {
		return
	}

	delete(reactorState.allocations, ptr)
	if len(reactorState.response) > 0 && bytesPointer(reactorState.response) == ptr {
		reactorState.response = nil
	}
}

//go:wasmexport stopgap_handle_request
func StopgapHandleRequest(ptr uint32, length uint32) int32 {
	if !reactorState.initialized {
		return storeProtocolError("reactor not initialized; call stopgap_init first")
	}

	raw := sliceAt(ptr, length)
	var req RequestEnvelope
	if err := json.Unmarshal(raw, &req); err != nil {
		return storeProtocolError(fmt.Sprintf("failed to decode request envelope: %v", err))
	}

	encoded, err := json.Marshal(HandleRequest(req))
	if err != nil {
		return storeProtocolError(fmt.Sprintf("failed to encode response envelope: %v", err))
	}

	reactorState.response = encoded
	return 0
}

//go:wasmexport stopgap_response_ptr
func StopgapResponsePtr() uint32 {
	if len(reactorState.response) == 0 {
		return 0
	}
	return bytesPointer(reactorState.response)
}

//go:wasmexport stopgap_response_len
func StopgapResponseLen() uint32 {
	return uint32(len(reactorState.response))
}

func storeProtocolError(message string) int32 {
	payload, err := json.Marshal(ResponseEnvelope{
		Diagnostics: []Diagnostic{{
			Severity: "error",
			Phase:    "protocol",
			Message:  message,
		}},
		Backend: "typescript-go",
	})
	if err != nil {
		return 1
	}

	reactorState.response = payload
	return 0
}

func bytesPointer(buf []byte) uint32 {
	if len(buf) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(&buf[0])))
}

func sliceAt(ptr uint32, length uint32) []byte {
	if ptr == 0 || length == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), int(length))
}
