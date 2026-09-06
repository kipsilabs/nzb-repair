package mocks

//go:generate go tool mockgen -source=../repairnzb/par2.go -destination=./par2_executor_mock.go -package=mocks
//go:generate go tool mockgen -source=../repairnzb/repair_nzb.go -destination=./nntp_pool_mock.go -package=mocks
