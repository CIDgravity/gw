package s3

import (
	"encoding/xml"
	"text/template"
)

const locationXml = `
<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{{.Region}}</LocationConstraint>
`

var locationTemplate = template.Must(template.New("location").Parse(locationXml))

type locationResponseParams struct {
	Region string
}

type ListObjectsEntry struct {
	Etag         string `xml:"ETag"`
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         uint64 `xml:"Size"`
}

type ListObjectsCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type ListObjectsResponse struct {
	XMLName     xml.Name           `xml:"ListBucketResult"`
	IsTruncated bool               `xml:"IsTruncated"`
	Contents    []ListObjectsEntry `xml:"Contents"`
	Name        string             `xml:"Name"`
	Prefix      string             `xml:"Prefix"`
	Delimiter   string             `xml:"Delimiter"`
	MaxKeys     int32              `xml:"MaxKeys"`

	CommonPrefixes []ListObjectsCommonPrefix `xml:"CommonPrefixes"`

	KeyCount              int32  `xml:"KeyCount"`
	ContinuationToken     string `xml:"ContinuationToken"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	StartAfter            string `xml:"StartAfter"`
}

const createMultipartUploadXml = `
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
   <Bucket>{{.Bucket}}</Bucket>
   <Key>{{.Key}}</Key>
   <UploadId>{{.UploadId}}</UploadId>
</InitiateMultipartUploadResult>
`

var createMultipartUploadTemplate = template.Must(template.New("createMultipartUpload").Parse(createMultipartUploadXml))

type createMultipartUploadResponseParams struct {
	Bucket   string
	Key      string
	UploadId string
}

const completeMultipartUploadXml = `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Location>location</Location>
   <Bucket>{{.Bucket}}</Bucket>
   <Key>{{.Key}}</Key>
   <ETag>{{.ETag}}</ETag>
</CompleteMultipartUploadResult>
`

var completeMultipartUploadTemplate = template.Must(template.New("completeMultipartUpload").Parse(completeMultipartUploadXml))

type completeMultipartUploadResponseParams struct {
	Bucket string
	Key    string
	ETag   string
}
