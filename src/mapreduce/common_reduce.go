package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	DecodedKeyValue := make(map[string][]string)
	for i := 0; i < nMap; i++ {

		indFileData, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			log.Fatal(err)
		}
		decoder := json.NewDecoder(indFileData)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			DecodedKeyValue[kv.Key] = append(DecodedKeyValue[kv.Key], kv.Value)
		}
		indFileData.Close()

	}
	/* runs but takes 400 seconds
	mergeFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(mergeFile)

	for key1, _ := range DecodedKeyValue {
		var key1Values []string
		for key2, value := range DecodedKeyValue {
			if key1 == key2 {
				key1Values = append(key1Values, value[0])
			}

		}
		enc.Encode(KeyValue{key1, reduceF(key1, key1Values)})
	}*/
	var differentKeys []string
	for k, _ := range DecodedKeyValue {
		differentKeys = append(differentKeys, k)
	}
	OutputFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(OutputFile)
	for _, key := range differentKeys {
		_ = enc.Encode(KeyValue{key, reduceF(key, DecodedKeyValue[key])})
	}

}
