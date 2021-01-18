package main

import (
	"fmt"
	"math"
	"time"
)

func main1() {
	secs := 10
	t1 := time.Now().Add(time.Second * time.Duration(secs))
	diff := time.Now().Sub(t1).Seconds()
	diffInt := math.Round(diff)
	fmt.Printf("Difference in time %v  and %v is %f \n", t1, time.Now(), diffInt)

}
