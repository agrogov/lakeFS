// Command bucket creates the S3 bucket the Metadata Search engine recipes write into.
//
// The engine recipes of docs/metadata-search.md need one bucket below the SeaweedFS instance of
// esti/ops/docker-compose-mds.yaml. lakeFS itself does not create it, and the recipe should not
// require the AWS CLI or a Python boto3 installation, so this small helper uses the minio-go client
// the repository already depends on.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const bucketOperationTimeout = 30 * time.Second

func main() {
	var (
		endpoint  = flag.String("endpoint", "http://127.0.0.1:8333", "S3 endpoint URL")
		accessKey = flag.String("access-key-id", "", "S3 access key ID")
		secretKey = flag.String("secret-access-key", "", "S3 secret access key")
		region    = flag.String("region", "us-east-1", "S3 region")
		bucket    = flag.String("bucket", "", "bucket to create")
	)
	flag.Parse()
	if *bucket == "" {
		failf("-bucket is required")
	}
	if *accessKey == "" || *secretKey == "" {
		failf("-access-key-id and -secret-access-key are required")
	}

	u, err := url.Parse(*endpoint)
	if err != nil || u.Host == "" {
		failf("parse endpoint %q: %v", *endpoint, err)
	}
	secure := u.Scheme == "https"
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(*accessKey, *secretKey, ""),
		Secure: secure,
		Region: *region,
	})
	if err != nil {
		failf("build S3 client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), bucketOperationTimeout)
	defer cancel()

	exists, err := client.BucketExists(ctx, *bucket)
	if err == nil && exists {
		fmt.Printf("bucket %q already exists\n", *bucket)
		return
	}
	if err := client.MakeBucket(ctx, *bucket, minio.MakeBucketOptions{Region: *region}); err != nil {
		// A concurrent run may have created it between the check and the create.
		if exists, checkErr := client.BucketExists(ctx, *bucket); checkErr == nil && exists {
			fmt.Printf("bucket %q already exists\n", *bucket)
			return
		}
		failf("create bucket %q: %v", *bucket, err)
	}
	fmt.Printf("bucket %q created\n", *bucket)
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "mds-bucket: "+format+"\n", args...)
	os.Exit(1)
}
