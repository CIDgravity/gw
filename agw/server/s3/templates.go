package s3

import (
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

const listObjectsXml = `
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{{.Name}}</Name>
    <Prefix/>
    <KeyCount>0</KeyCount>
    <MaxKeys>1</MaxKeys>
    <IsTruncated>false</IsTruncated>
</ListBucketResult>
`

var listObjectsTemplate = template.Must(template.New("listObjects").Parse(listObjectsXml))

type listObjectsResponseParams struct {
	Name string
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
