package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
)

const (
	bucket        = ""
	endpoint      = ""
	region        = ""
	access_key    = ""
	access_secret = ""
)

func main() {
	fmt.Println("hello")
	s3Client := GetS3Client()
	progress := make(chan int64)

	go func() {
		// 使用一个变量lastProgress来记录上一次打印的进度值，避免重复打印相同的进度。通过range关键字遍历progress通道，获取进度值，并进行打印。
		lastProgress := int64(0)
		for p := range progress {
			if p == lastProgress {
				continue
			}
			lastProgress = p
			fmt.Printf("文件上传s3进度: %d\n", p)
		}
	}()
	key := "Default.jpg"
	key, upload_res, err := s3Client.PutFileMultiPart(key, "./Default.jpg", bucket, true, progress)
	if err != nil {
		fmt.Println("上传到s3失败", err)
		return
	}
	fmt.Println("上传到s3成功", key, upload_res)
	geturl, err := s3Client.SignedGetURL(key, bucket, 600*time.Second)
	if err != nil {
		fmt.Println("获取下载地址", err)
		return
	}
	fmt.Println(geturl)
}

func GetS3Client() *S3Provider {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	insecureHTTPClient := &http.Client{Transport: tr}
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(access_key, access_secret, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		HTTPClient:       insecureHTTPClient,
		S3ForcePathStyle: aws.Bool(true),
	}
	s, err := session.NewSession(cfg)
	if err != nil {
		logrus.Errorf("NewS3Provider NewSession failed %v", err)
	}
	client := s3.New(s)
	return &S3Provider{
		Client:  client,
		Session: s,
		Config:  cfg,
	}
}

type S3Provider struct {
	Client  *s3.S3
	Session *session.Session
	Config  *aws.Config
}

// PutFileMultiPart 打开要上传的文件，并获取文件的信息。然后，创建一个ProgressReader结构体对象，将文件和进度通道传入。该结构体实现了ReadAt()方法，用于读取文件并更新上传进度。
func (p *S3Provider) PutFileMultiPart(key, filename, bucket string, public bool, progress chan int64) (path, result string, err error) {
	// upload to s3 with progress
	acl := s3.ObjectCannedACLAuthenticatedRead
	if public {
		acl = s3.ObjectCannedACLPublicRead
	}
	file, err := os.Open(filename)
	if err != nil {
		return "", "", err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return "", "", err
	}
	reader := &ProgressReader{
		fp:       file,
		size:     fileInfo.Size(),
		progress: progress,
	}

	uploader := s3manager.NewUploader(p.Session)
	up, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		ACL:    aws.String(acl),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		logrus.Errorf("file upload error:%v", err)
		return "", "", err
	}
	upStr, _ := json.Marshal(up)
	return key, string(upStr), nil
}

func (p *S3Provider) PublicURL(key, bucket string) string {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, _ := p.Client.GetObjectRequest(input)
	_ = req.Build()
	return req.HTTPRequest.URL.String()
}

func (p *S3Provider) SignedGetURL(key, bucket string, expire time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	req, _ := p.Client.GetObjectRequest(input)
	return req.Presign(expire)
}

// 创建一个ProgressReader结构体对象，将文件和进度通道传入。该结构体实现了ReadAt()方法，用于读取文件并更新上传进度
type ProgressReader struct {
	fp       *os.File
	size     int64
	read     int64
	progress chan int64
}

func (r *ProgressReader) Read(p []byte) (int, error) {
	return r.fp.Read(p)
}

// 实现了ReadAt()方法，用于读取文件并更新上传进度
// 首先调用fp.ReadAt()方法读取文件内容，并将读取的字节数保存到变量n中。
// 然后，使用atomic包中的AddInt64()函数将读取的字节数加到r.read变量中，表示已读取的字节数。
// 接着，计算上传进度的百分比，并将结果发送到进度通道中。
func (r *ProgressReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.fp.ReadAt(p, off)
	if err != nil {
		return n, err
	}

	// Got the length have read( or means has uploaded), and you can construct your message
	atomic.AddInt64(&r.read, int64(n))

	// I have no idea why the read length need to be div 2,
	// maybe the request read once when Sign and actually send call ReadAt again
	// It works for me

	// check if progress channel is closed
	r.progress <- int64(float64(r.read/2) / float64(r.size) * 100)
	return n, err
}

func (r *ProgressReader) Seek(offset int64, whence int) (int64, error) {
	return r.fp.Seek(offset, whence)
}
