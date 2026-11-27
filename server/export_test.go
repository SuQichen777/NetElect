package server

// Helpers exported only for tests, for use by package server_test.

var (
	RttMeanForTest      = rttMean
	RttStdDevForTest    = rttStdDev
	JacobsonRTOForTest  = jacobsonRTO
	CIUpperBoundForTest = ciUpperBound
)
