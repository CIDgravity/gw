package s3

import "time"

func FormatDateTime(dateTime time.Time) string {
	location := time.FixedZone("GMT", 0)
	return dateTime.In(location).Format("Mon, 2 Jan 2006 15:04:05 MST")
}
