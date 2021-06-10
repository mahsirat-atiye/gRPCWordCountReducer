package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//mapper


func mapperMapper(filename interface{}, output chan interface{}) {

	results := make(map[int][]string)

	// read file word-by-word
	for word := range myReadFile(filename.(string)) {
		reminder := int(word[0]) % numOfComplementaryTasks

		// stores the result
		results[reminder] = append(results[reminder], word)

	}

	output <- results
}


func mapperReducer(input chan interface{}, output chan interface{}) {

	results := map[string]int{}
	for matches := range input {
		for reminder, listOfStrings := range matches.(map[int][]string) {
			targetFile := fileAddress(reminder, taskIndex)
			filehandle, err := createOrAppend(targetFile)
			if err != nil {
				fmt.Println("Error writing to file: ", err)
				return
			}
			fmt.Println("Writing to file:", filehandle.Name())

			defer filehandle.Close()

			writer := bufio.NewWriter(filehandle)

			for _, word := range listOfStrings {
				fmt.Fprintln(writer, word)
			}
			writer.Flush()
			filehandle.Close()
		}
	}
	output <- results
}

func mapperMapReduce(mapper MapperFunction, reducer ReducerFunction, input chan interface{}) interface{} {

	reducerInput := make(chan interface{})
	reducerOutput := make(chan interface{})
	MapCollector := make(MapCollector, MaxThreads)

	go reducer(reducerInput, reducerOutput)
	go reducerDispatcher(MapCollector, reducerInput)
	go mapperDispatcher(mapper, input, MapCollector)

	return <-reducerOutput
}
func fileAddress(reminder int, id int) string {
	return intermediateDirPath + "/mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(reminder) + ".txt"
}



func mapperMain() {

	starttime := time.Now()

	fmt.Println("Processing . . .")

	// start the enumeration of files to be processed into a channel
	input := getFiles(inputDirPath, targetFiles)

	// this will start the map reduce work
	mapperMapReduce(mapperMapper, mapperReducer, input)

	elapsedtime := time.Since(starttime)
	fmt.Println("Complete!!")
	fmt.Println("Time taken:", elapsedtime)

}

// Reducer
func reducerMapper(filename interface{}, output chan interface{}) {

	results := map[string]int{}

	// read file word-by-word
	for word := range myReadFile(filename.(string)) {

		// stores the result
		results[strings.ToLower(word)] += 1

	}

	output <- results
}

func reducerReducer(input chan interface{}, output chan interface{}) {

	results := map[string]int{}
	for matches := range input {
		for word, frequency := range matches.(map[string]int) {
			results[strings.ToLower(word)] += frequency
		}
	}
	output <- results
}

func reducerMapReduce(mapper MapperFunction, reducer ReducerFunction, input chan interface{}) interface{} {

	reducerInput := make(chan interface{})
	reducerOutput := make(chan interface{})
	MapCollector := make(MapCollector, MaxThreads)

	go reducer(reducerInput, reducerOutput)
	go reducerDispatcher(MapCollector, reducerInput)
	go mapperDispatcher(mapper, input, MapCollector)

	return <-reducerOutput
}


func reducerMain() {

	starttime := time.Now()

	fmt.Println("Processing . . .")


	// start the enumeration of files to be processed into a channel
	input := getFiles(intermediateDirPath, targetFiles)

	// this will start the map reduce work
	results := reducerMapReduce(reducerMapper, reducerReducer, input)

	filehandle, err := createOrAppend(outputDirPath + "/out-"+strconv.Itoa(taskIndex)+".txt")
	if err != nil {
		fmt.Println("Error writing to file: ", err)
		return
	}
	fmt.Println("Writing to file:", filehandle.Name())

	defer filehandle.Close()

	writer := bufio.NewWriter(filehandle)

	for word, frequency := range results.(map[string]int) {
		fmt.Fprintln(writer, word+" "+strconv.Itoa(frequency))
	}
	writer.Flush()
	filehandle.Close()

	elapsedtime := time.Since(starttime)
	fmt.Println("Complete!!")
	fmt.Println("Time taken:", elapsedtime)

}

// Common functionalities

type MapCollector chan chan interface{}

type MapperFunction func(interface{}, chan interface{})

type ReducerFunction func(chan interface{}, chan interface{})

const (
	MaxThreads = 8
)

func myReadFile(filename string) chan string {
	output := make(chan string)
	reg, _ := regexp.Compile("[^A-Za-z0-9]+")

	go func() {
		fmt.Println("Reading file : " + filename + "\n")
		file, err := os.Open(filename)
		if err != nil {
			return
		}
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanWords)

		// Scan all words from the file.
		for scanner.Scan() {
			//remove all spaces and special chars
			word := strings.TrimSpace(reg.ReplaceAllString(scanner.Text(), ""))
			if len(word) > 0 {
				output <- word
			}
		}
		defer file.Close()

		close(output)
		fmt.Println("Completed file : " + filename + "\n")

	}()
	return output
}

func getFiles(dirname string, targetFiles []string) chan interface{} {
	output := make(chan interface{})
	go func() {
		filepath.Walk(dirname, func(path string, f os.FileInfo, err error) error {
			if !f.IsDir() {
				if contains(targetFiles, path){
					fmt.Println(path)
					output <- path
				}
			}
			return nil
		})
		close(output)
	}()
	return output
}

func reducerDispatcher(collector MapCollector, reducerInput chan interface{}) {

	for output := range collector {
		reducerInput <- <-output
	}
	close(reducerInput)
}

func mapperDispatcher(mapper MapperFunction, input chan interface{}, collector MapCollector) {

	for item := range input {
		taskOutput := make(chan interface{})
		go mapper(item, taskOutput)
		collector <- taskOutput

	}
	close(collector)
}

func createOrAppend(p string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0770); err != nil {
		return nil, err
	}
	return os.OpenFile(p, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0775)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
