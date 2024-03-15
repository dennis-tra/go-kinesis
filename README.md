# Go Kinesis Producer Library
[![standard-readme compliant][standard-readme-img]][standard-readme-url]
[![GoDoc][godoc-img]][godoc-url]


A Kinesis batch producer library for [Amazon Kinesis][kinesis-url] built on top
of the official Go AWS SDK and using the same aggregation format as the
[Kinesis Producer Library (KPL)][kpl-url].

Features

- **Record Aggregation**: 
- **Backpressure**
- **Rate Limit Aware**
- **Shard Watcher**
- **Custom Logging**
- **Telemetry**

## Table of Contents

- [Go Kinesis Producer Library](#go-kinesis-producer-library)
	- [Table of Contents](#table-of-contents)
	- [Background](#background)
	- [Installation](#installation)
	- [Usage](#usage)

## Background

## Installation


## Usage


```go
import gk "github.com/dennis-tra/go-kinesis"

func main() {
	
	awsConfig, _ := config.LoadDefaultConfig(ctx)
	client := kinesis.NewFromConfig(awsConfig)
	
	p, err := NewProducer(client, "my-stream", gk.DefaultProducerConfig())

	ctx, stop := context.WithCancel(context.Background())
	go p.Start(ctx)

	p.Put(ctx, "partition-key", []byte("payload"))

	stop()

}
```


[kinesis-url]: https://aws.amazon.com/kinesis/
[kpl-url]: https://github.com/awslabs/amazon-kinesis-producer
[standard-readme-url]: https://github.com/RichardLitt/standard-readme
[standard-readme-img]: https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square
[godoc-url]: https://godoc.org/github.com/dennis-tra/go-kinesis
[godoc-img]: https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square