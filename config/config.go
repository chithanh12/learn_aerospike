package config

type AerospikeConfig struct {
	Host      string
	Port      int
	Namespace string
	Udf       string
}
