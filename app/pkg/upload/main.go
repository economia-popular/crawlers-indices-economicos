package upload

import (
	"bytes"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func S3(source string, key string) error {

	session, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		log.Fatal(err)
		return err
	}

	upFile, err := os.Open(source)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer upFile.Close()

	upFileInfo, _ := upFile.Stat()
	var fileSize int64 = upFileInfo.Size()
	fileBuffer := make([]byte, fileSize)
	upFile.Read(fileBuffer)

	_, err = s3.New(session).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String("economia-popular-delivery-content-indices"),
		Key:                  aws.String(key),
		ACL:                  aws.String("public-read"),
		Body:                 bytes.NewReader(fileBuffer),
		ContentLength:        aws.Int64(fileSize),
		ContentType:          aws.String(http.DetectContentType(fileBuffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err

}
