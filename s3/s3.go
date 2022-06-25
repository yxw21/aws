package s3

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/url"
)

type S3 struct {
	client *s3.Client
}

func (ctx *S3) CreateBucket(bucket string) (*s3.CreateBucketOutput, error) {
	input := &s3.CreateBucketInput{
		Bucket: &bucket,
	}
	return ctx.client.CreateBucket(context.TODO(), input)
}

func (ctx *S3) DeleteBucket(bucket string) (*s3.DeleteBucketOutput, error) {
	input := &s3.DeleteBucketInput{
		Bucket: &bucket,
	}
	return ctx.client.DeleteBucket(context.TODO(), input)
}

func (ctx *S3) UploadObject(bucket, key string, body io.Reader, contentType, cacheControl string) (*manager.UploadOutput, error) {
	uploader := manager.NewUploader(ctx.client)
	return uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         body,
		ContentType:  aws.String(contentType),
		CacheControl: &cacheControl,
	})
}

func (ctx *S3) DeleteObject(bucket, key string) (*s3.DeleteObjectOutput, error) {
	input := &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}
	return ctx.client.DeleteObject(context.TODO(), input)
}

func (ctx *S3) CopyObject(sourceBucket, key, destinationBucket string) (*s3.CopyObjectOutput, error) {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(url.PathEscape(sourceBucket)),
		CopySource: &destinationBucket,
		Key:        &key,
	}
	return ctx.client.CopyObject(context.TODO(), input)
}

func (ctx *S3) GetObjectSign(bucket, key string) (*v4.PresignedHTTPRequest, error) {
	preSignClient := s3.NewPresignClient(ctx.client)
	return preSignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func (ctx *S3) ListBuckets() (*s3.ListBucketsOutput, error) {
	input := &s3.ListBucketsInput{}
	return ctx.client.ListBuckets(context.TODO(), input)
}

func (ctx *S3) ListObjects(bucket string) (*s3.ListObjectsOutput, error) {
	input := &s3.ListObjectsInput{
		Bucket: &bucket,
	}
	return ctx.client.ListObjects(context.TODO(), input)
}

func (ctx *S3) GetClient() *s3.Client {
	return ctx.client
}

func NewS3(region, key, secret, session string) (*S3, error) {
	if region == "" {
		return nil, errors.New("region cannot be empty")
	}
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}
	if secret == "" {
		return nil, errors.New("secret cannot be empty")
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, session)),
	)
	if err != nil {
		return nil, err
	}
	return &S3{
		client: s3.NewFromConfig(cfg),
	}, nil
}
