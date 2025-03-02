package util

import "strings"

func SplitAndTrim(input string, sep string) []string {
	if IsEmpty(input) {
		return make([]string, 0)
	}
	parts := strings.Split(input, sep)
	results := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			results = append(results, trimmed)
		}
	}
	return results
}

func IsEmpty(input string) bool {
	return strings.TrimSpace(input) == ""
}
