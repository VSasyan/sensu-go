package graphql

// clampInt returns int within given range.
func clampInt(num, min, max int) int {
	if num < min {
		return min
	} else if num > max {
		return max
	}
	return num
}