package main

import (
	"encoding/json"
	"fmt"
	"github.com/abaeve/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
)

type arrayWrapper struct {
	Children []map[string]interface{}
}

func main() {
	yamlFile, err := ioutil.ReadFile("typeIDs.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err #%v ", err)
	}

	jsonable := new(interface{})
	utils.Unmarshal(yamlFile, jsonable)

	jsonCleaned := interface{}(*jsonable).(map[string]interface{})
	jsonToMarshal := arrayWrapper{}

	//TODO: EDIT
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	c := session.DB("sde").C("types")

	index := mgo.Index{
		Key:        []string{"name", "typeID"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     false,
	}
	err = c.EnsureIndex(index)
	if err != nil {
		panic(err)
	}

	bulk := session.DB("sde").C("types").Bulk()
	documentCount := 0

	//make the input an array of objects with a typeID field
	for key, value := range jsonCleaned {
		valueC := value.(map[string]interface{})
		valueC["typeID"] = key
		jsonToMarshal.Children = append(jsonToMarshal.Children, value.(map[string]interface{}))

		//strip out unwanted locales to save runtime memory
		if valueC["description"] != nil {
			//TODO: EDIT
			valueC["description"] = valueC["description"].(map[string]interface{})["en"]
		}

		if valueC["name"] != nil {
			//TODO: EDIT
			valueC["name"] = valueC["name"].(map[string]interface{})["en"]
		}

		//delete unwanted attributes to further trim memory usage
		delete(valueC, "traits")
		delete(valueC, "masteries")

		bulk.Upsert(bson.M{"_id": key}, valueC)

		documentCount += 1
		//fmt.Printf("Document count %d\n", documentCount)
		if documentCount >= 999 {
			//fmt.Println("Persisting stats...")
			_, err := bulk.Run()
			if err != nil {
				fmt.Errorf("errored saving stats: %s\n", err.Error())
			}

			documentCount = 0
			sessionN := session.Copy()
			session.Close()
			session = sessionN
			bulk = session.DB("sde").C("types").Bulk()
		}
	}

	_, err = bulk.Run()
	if err != nil {
		fmt.Errorf("errored saving stats: %s\n", err.Error())
	}

	typeIDsJSON, _ := json.MarshalIndent(jsonToMarshal.Children, "", "  ")
	err = ioutil.WriteFile("typeIDs.json", typeIDsJSON, 0644)
	if err != nil {
		log.Printf("typeIDs.Post err   #%v ", err)
	}
}
