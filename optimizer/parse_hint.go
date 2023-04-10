package optimizer

import (
	"regexp"
	"strconv"
	"strings"
)

type Hint struct {
	PartitionNumber int
}

func ParseHint(sql string) Hint {
	hint := Hint{}
	match := regexp.MustCompile(`/\*\+(.*?)\*/`).FindStringSubmatch(sql)
	if len(match) == 2 {
		commaHint := strings.Split(strings.Trim(match[1], " "), ",")
		for _, equalHint := range commaHint {
			s := strings.Split(equalHint, "=")
			if len(s) == 2 {
				if s[0] == "partition_number" {
					hint.PartitionNumber, _ = strconv.Atoi(s[1])
				}
			}
		}
	}
	return hint
}
