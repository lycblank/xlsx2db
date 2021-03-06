package db

import "bytes"

func SnakeName(name string) string {
	var buf bytes.Buffer
	for _, c := range []byte(name) {
		if c >= 'A' && c <= 'Z' {
			if buf.Len() > 0 {
				buf.WriteByte('_')
			}
			c += 'a' - 'A'
		}
		buf.WriteByte(c)
	}
	return buf.String()
}

func CaseName(name string) string {
	trans := false
	var buf bytes.Buffer
	for _, c := range []byte(name) {
		if buf.Len() == 0 {
			if c >= 'a' && c <= 'z' {
				trans = true
			}
		}
		if c == '_' {
			trans = true
			continue
		}

		if trans {
			c -= 'a' - 'A'
			trans = false
		}
		buf.WriteByte(c)
	}
	return buf.String()
}

func FirstLowerName(name string) string {
	if len(name) <= 0 {
		return name
	}
	datas := []byte(name)
	if datas[0] >= 'A' && datas[0] <= 'Z' {
		datas[0] += 'a' - 'A'
	}
	return string(datas)
}