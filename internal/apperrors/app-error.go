package apperrors

import "fmt"

type AppError struct {
	Code    ErrCode
	Message string
	Err     error
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%d | %s - %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%d | %s", e.Code, e.Message)
}

func New(code ErrCode, message string, err error) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}
