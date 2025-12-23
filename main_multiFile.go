// eg query : http://localhost:8080/getData?from=1759276800&till=1766054889000&filterOut=value, name&subtopic=motor/rpm
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	AccessKey = ""
	SecretKey = ""
	Endpoint  = "https://blr1.digitaloceanspaces.com"
	Region    = "blr1"
	Bucket    = "local-dev-test-bucket"
)

type S3File struct {
	Name   string
	Client *s3.Client
	Bucket string
	Key    string
	Size   int64
	Offset int64
}

type Data struct {
	Timestamp int64   `json:"timestamp"`
	Subtopic  string  `json:"subtopic,omitempty"`
	Name      string  `json:"name,omitempty"`
	Value     float64 `json:"value,omitempty"`
}

func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= f.Size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= f.Size {
		end = f.Size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)

	resp, err := f.Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &f.Bucket,
		Key:    &f.Key,
		Range:  &rangeHeader,
	})

	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return io.ReadFull(resp.Body, p)
}

func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.Offset + offset
	case io.SeekEnd:
		newOffset = f.Size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	f.Offset = newOffset
	return newOffset, nil
}

func CreateS3Client(ctx context.Context) (*s3.Client, error) {
	creds := credentials.NewStaticCredentialsProvider(AccessKey, SecretKey, "")

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(Region),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &[]string{Endpoint}[0]
	})

	fmt.Println("Connected to S3 (DigitalOcean)")
	return client, nil
}

func ListOfFiles(ctx context.Context, client *s3.Client, bucket string, fromMs int64, tillMs int64) ([]*S3File, error) {
	var validFiles []*S3File

	startDate := time.UnixMilli(fromMs).UTC().Format("2006-01-02")
	endDate := time.UnixMilli(tillMs).UTC().Format("2006-01-02")

	fmt.Printf("Filtering files between [Date: %s] and [Date: %s]\n", startDate, endDate)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("data_"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			fileDateStr := parseDateFromKey(key)
			if fileDateStr == "" {
				continue
			}

			if fileDateStr >= startDate && fileDateStr <= endDate {
				validFiles = append(validFiles, &S3File{
					Name:   key,
					Client: client,
					Bucket: bucket,
					Key:    key,
					Size:   aws.ToInt64(obj.Size),
					Offset: 0,
				})
			}
		}
	}
	return validFiles, nil
}

func parseDateFromKey(key string) string {
	parts := strings.Split(key, "/")
	fileName := parts[len(parts)-1]
	if !strings.HasPrefix(fileName, "data_") || !strings.HasSuffix(fileName, ".parquet") {
		return ""
	}
	datePart := strings.TrimPrefix(fileName, "data_")
	return strings.TrimSuffix(datePart, ".parquet")
}

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, s) { 
			return true
		}
	}
	return false
}

func readSingleFile(s3file *S3File, subtopic string, name string, value string, filterOut []string) ([]Data, error) {

	if subtopic != "" && containsString(filterOut, "subtopic") {
		return nil, fmt.Errorf("filter conflict: 'subtopic'")
	}
	if name != "" && containsString(filterOut, "name") {
		return nil, fmt.Errorf("filter conflict: 'name'")
	}
	if value != "" && containsString(filterOut, "value") {
		return nil, fmt.Errorf("filter conflict: 'value'")
	}

	pr, err := file.NewParquetReader(s3file)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.Close()

	var dataList []Data

	for rg := 0; rg < pr.NumRowGroups(); rg++ {
		current_row := pr.RowGroup(rg)

		time_col, _ := current_row.Column(0)
		subtopic_col, _ := current_row.Column(2)
		name_col, _ := current_row.Column(5)
		value_col, _ := current_row.Column(7)

		timeReader := time_col.(*file.Int64ColumnChunkReader)
		subtopicReader := subtopic_col.(*file.ByteArrayColumnChunkReader)
		nameReader := name_col.(*file.ByteArrayColumnChunkReader)
		valueReader := value_col.(*file.Float64ColumnChunkReader)

		numRows := int(current_row.NumRows())

		timeValues := make([]int64, numRows)
		timeRead, _, _ := timeReader.ReadBatch(int64(numRows), timeValues, nil, nil)

		subtopicRead := int64(timeRead)
		nameRead := int64(timeRead)
		valueRead := int64(timeRead)

		var subtopicValues []parquet.ByteArray
		var nameValues []parquet.ByteArray
		var valueValues []float64

		if !containsString(filterOut, "subtopic") {
			subtopicValues = make([]parquet.ByteArray, numRows)
			subtopicRead, _, _ = subtopicReader.ReadBatch(int64(numRows), subtopicValues, nil, nil)
		}
		if !containsString(filterOut, "name") {
			nameValues = make([]parquet.ByteArray, numRows)
			nameRead, _, _ = nameReader.ReadBatch(int64(numRows), nameValues, nil, nil)
		}
		if !containsString(filterOut, "value") {
			valueValues = make([]float64, numRows)
			valueRead, _, _ = valueReader.ReadBatch(int64(numRows), valueValues, nil, nil)
		}

		if (!containsString(filterOut, "subtopic") && timeRead != subtopicRead) ||
			(!containsString(filterOut, "name") && timeRead != nameRead) ||
			(!containsString(filterOut, "value") && timeRead != valueRead) {
			return nil, fmt.Errorf("mismatched read counts in row group %d", rg)
		}

		for i := 0; i < int(timeRead); i++ {
			time_in_sec := timeValues[i]
			if time_in_sec < 1_000_000_000_000 {
				time_in_sec = time_in_sec * 1000
			}

			matchSubtopic := true
			if subtopic != "" && string(subtopicValues[i]) != subtopic {
				matchSubtopic = false
			}

			matchName := true
			if matchSubtopic && name != "" && string(nameValues[i]) != name {
				matchName = false
			}

			matchValue := true
			if matchSubtopic && matchName && value != "" && fmt.Sprintf("%f", valueValues[i]) != value {
				matchValue = false
			}

			if matchSubtopic && matchName && matchValue {
				record := Data{Timestamp: time_in_sec}

				if !containsString(filterOut, "subtopic") {
					record.Subtopic = string(subtopicValues[i])
				}
				if !containsString(filterOut, "name") {
					record.Name = string(nameValues[i])
				}
				if !containsString(filterOut, "value") {
					record.Value = valueValues[i]
				}
				dataList = append(dataList, record)
			}
		}
	}

	return dataList, nil
}

func getData(ctx context.Context, client *s3.Client, from_time int64, till_time int64, subtopic string, name string, value string, filterOut []string) ([]Data, error) {

	if (subtopic != "" && containsString(filterOut, "subtopic")) ||
		(name != "" && containsString(filterOut, "name")) ||
		(value != "" && containsString(filterOut, "value")) {
		return nil, fmt.Errorf("can't filter by a field that is excluded")
	}

	namesList, err := ListOfFiles(ctx, client, Bucket, from_time, till_time)
	if err != nil {
		return nil, err
	}

	var dataRet []Data
	for _, nameL := range namesList {
		data, err := readSingleFile(nameL, subtopic, name, value, filterOut)
		if err != nil {
			fmt.Printf("Error reading file %s: %v\n", nameL.Name, err)
			continue
		}
		dataRet = append(dataRet, data...)
	}

	return dataRet, nil
}

func apiGetData(client *s3.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startStr := r.URL.Query().Get("from")
		endStr := r.URL.Query().Get("till")

		endMs := time.Now().UnixMilli()
		startMs := time.Now().AddDate(0, 0, -5).UnixMilli()

		if s, err := strconv.ParseInt(startStr, 10, 64); err == nil {
			startMs = s
		}
		if e, err := strconv.ParseInt(endStr, 10, 64); err == nil {
			endMs = e
		}

		subtopic := r.URL.Query().Get("subtopic")
		name := r.URL.Query().Get("name")
		value := r.URL.Query().Get("value")

		rawFilter := r.URL.Query().Get("filterOut")
		var filterOut []string
		if rawFilter != "" {
			filterOut = strings.Split(rawFilter, ",")
			for i := range filterOut {
				filterOut[i] = strings.TrimSpace(filterOut[i])
			}
		}

		data, err := getData(r.Context(), client, startMs, endMs, subtopic, name, value, filterOut)

		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting data: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

func main() {
	ctx := context.Background()

	s3Client, err := CreateS3Client(ctx)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/getData", apiGetData(s3Client))

	fmt.Println("Server is running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
