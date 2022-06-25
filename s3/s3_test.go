package s3

import (
	"bytes"
	"log"
	"testing"
)

func TestS3(t *testing.T) {
	bucket := "test"

	// init
	s3, err := NewS3("us-east-2", "{key}", "{secret}", "{session}")
	if err != nil {
		t.Error(err)
	}

	// upload
	uploadOutput, err := s3.UploadObject(bucket, "test.txt", bytes.NewBufferString("test"), "text/plain", "public, max-age=14400")
	if err != nil {
		t.Error(err)
	}
	log.Println(uploadOutput.Location)

	// get object signURL
	preSignedHTTPRequest, err := s3.GetObjectSign(bucket, *uploadOutput.Key)
	if err != nil {
		t.Error(err)
	}
	log.Println(preSignedHTTPRequest.URL)

	// list buckets
	listBucketsOutput, err := s3.ListBuckets()
	if err != nil {
		t.Error(err)
	}
	log.Println(listBucketsOutput.Buckets)

	// list objects
	listObjectsOutput, err := s3.ListObjects(bucket)
	if err != nil {
		t.Error(err)
	}
	log.Println(listObjectsOutput.Contents)

	// delete object
	_, err = s3.DeleteObject(bucket, "test.txt")
	if err != nil {
		t.Error(err)
	}
}
