package rpc

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NotFoundViewErr() *Error {
	return &Error{
		Code:    44404,
		Message: "Not found view",
	}
}
