package redis

import "crypto/tls"

func defaultTLSConfig() *tls.Config { return &tls.Config{MinVersion: tls.VersionTLS12} }
