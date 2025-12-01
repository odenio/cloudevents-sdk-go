module github.com/cloudevents/sdk-go/samples/http/sender-avro

go 1.25.4

replace github.com/cloudevents/sdk-go/v2 => ../../../v2

replace github.com/cloudevents/sdk-go/binding/format/avro/v2 => ../../../binding/format/avro/v2

require (
	github.com/cloudevents/sdk-go/binding/format/avro/v2 v2.0.0-00010101000000-000000000000
	github.com/cloudevents/sdk-go/v2 v2.16.2
	github.com/hamba/avro/v2 v2.30.0
)

require (
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
)
