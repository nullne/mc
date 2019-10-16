module github.com/minio/mc

go 1.12

require (
	github.com/beltran/gohive v0.0.0-20190708233207-81b899b60449
	github.com/cheggaaa/pb v1.0.28
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.7.0
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/gopherjs/gopherjs v0.0.0-20190430165422-3e4dfb77656c // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/hashicorp/go-version v1.1.0
	github.com/howeyc/gopass v0.0.0-20190910152052-7cb4b85ec19c // indirect
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/mattn/go-colorable v0.1.1
	github.com/mattn/go-isatty v0.0.7
	github.com/minio/cli v1.21.0
	github.com/minio/minio v0.0.0-20190903181048-8a71b0ec5a72
	github.com/minio/minio-go v0.0.0-20190327203652-5325257a208f
	github.com/minio/sha256-simd v0.1.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/profile v1.3.0
	github.com/pkg/xattr v0.4.1
	github.com/posener/complete v1.2.2-0.20190702141536-6ffe496ea953
	github.com/rjeczalik/notify v0.9.2
	github.com/segmentio/go-prompt v1.2.1-0.20161017233205-f0d19b6901ad
	github.com/segmentio/kafka-go v0.3.1
	github.com/smartystreets/assertions v1.0.0 // indirect
	github.com/smartystreets/goconvey v0.0.0-20190710185942-9d28bd7c0945 // indirect
	github.com/tidwall/match v1.0.1 // indirect
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/tsuna/gohbase v0.0.0-20190809153024-c154997cc002 // indirect
	gitlab.p1staff.com/common-tech/tantan-object-storage v1.0.3
	gitlab.p1staff.com/common-tech/tantan-object-storage/cluster v1.0.0
	go.uber.org/multierr v1.1.0
	golang.org/x/crypto v0.0.0-20190923035154-9ee001bba392
	golang.org/x/net v0.0.0-20190923162816-aa69164e4478
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20181108054448-85acf8d2951c
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/cheggaaa/pb.v1 v1.0.28 // indirect
	gopkg.in/h2non/filetype.v1 v1.0.5
)

// replace github.com/minio/minio => gitlab.p1staff.com/common-tech/minio v1.1.4
replace github.com/minio/minio => /root/workspace/go/src/github.com/minio/minio
