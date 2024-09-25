package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	var token = os.Getenv("TINYBIRD_TOKEN")
	if token == "" {
		fmt.Printf("error getting env variable TINYBIRD_TOKEN\n")
		os.Exit(1)
	}

	res, err := http.Get("https://api-pgics.sevilla.org/request/count_last_days?last_days=1&jurisdiction_ids=org.sevilla")
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
		os.Exit(1)
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalln(err)
	}

	numberOfIncidencesLast24Hours := gjson.Get(
		string(body),
		"count",
	)

	log.Printf("count: %s", numberOfIncidencesLast24Hours)

	incidencesRes, err := http.Get(fmt.Sprintf("https://api-pgics.sevilla.org/requests?jurisdiction_ids=org.sevilla&limit=%s", numberOfIncidencesLast24Hours))
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
		os.Exit(1)
	}

	defer incidencesRes.Body.Close()

	incidencesResBody, err := io.ReadAll(incidencesRes.Body)
	if err != nil {
		log.Fatalln(err)
	}

	resBytes := []byte(string(incidencesResBody)) // Converting the string "res" into byte array
	var resArr []map[string]interface{}           // declaring a map for key names as string and values as interface
	err = json.Unmarshal(resBytes, &resArr)
	if err != nil {
		log.Fatalln(err)
	}

	for i := range resArr {
		id := resArr[i]["service_id"].(string)
		log.Printf(id)
		incidenceType := resArr[i]["service_name"].(string)
		log.Printf(incidenceType)
		description := resArr[i]["description"].(string)
		log.Printf(description)
		requestedDate := resArr[i]["requested_datetime"].(string)
		log.Printf(requestedDate)
		address := resArr[i]["address"].(string)
		log.Printf(address)
		latitude := resArr[i]["lat"].(float64)
		log.Printf(fmt.Sprintf("%f", latitude))
		longitude := resArr[i]["long"].(float64)
		log.Printf(fmt.Sprintf("%f", longitude))

		incidence := IncidenceEvent{CreatedAt: time.Now().Unix(), Id: id, Type: incidenceType, Description: description, RequestedDate: requestedDate, Address: address, Latitude: latitude, Longitude: longitude}
		sendEventToTinyBird(incidence)
	}

}

type IncidenceEvent struct {
	CreatedAt     int64   `json:"timestamp"`
	Id            string  `json:"id"`
	Type          string  `json:"type"`
	Description   string  `json:"description"`
	RequestedDate string  `json:"requestedDate"`
	Address       string  `json:"address"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
}

func sendEventToTinyBird(incidence IncidenceEvent) {
	url := "https://api.eu-central-1.aws.tinybird.co/v0/events?name=incidences"

	b, err := json.Marshal(incidence)
	if err != nil {
		fmt.Println(err)
		return
	}

	var jsonStr = []byte(string(b))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))

	sprintf := fmt.Sprintf("Bearer %s", os.Getenv("TINYBIRD_TOKEN"))
	req.Header.Set("Authorization", sprintf)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Second * 10}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}
