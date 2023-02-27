package main

/*
@author 1Zero64
Main program for materializer to transform measurements into a materialized view
*/

// Importing packages
import (
	// Package to use SQL-like databases
	"database/sql"
	// Package for sorting Slices
	"sort"
	// Package for formatted printing
	"fmt"
	// Package with interface to operating system functionality
	"os"
	// Package for measuring and displaying time values
	"time"
	// Package for math functions
	"math"

	// Package for .env functionality
	"github.com/joho/godotenv"

	// Package to use PostgreSQL database
	_ "github.com/lib/pq"

	// Package for progress bar
	"github.com/schollz/progressbar/v3"
)

// Enumerations for danger level
const (
	No       = "No"
	Low      = "Low"
	Medium   = "Medium"
	High     = "High"
	Critical = "Critical"
)

/*
Implicitly called function on initialization of the main application
Executed only once and before main()
*/
func init() {

	// Load .env variables and check on error with handler
	if err := godotenv.Load(); err != nil {
		checkError(err)
	}
}

/*
Entry point of the executable program
Triggers the materialize process
*/
func main() {

	// Build connection string to Postgres database with the database information from .env variables
	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_DATABASE"))

	// Open database and check on error with handler
	db, err := sql.Open("postgres", psqlconn)
	checkError(err)

	// Print info on successfull connection
	fmt.Println("Connected with database!")

	// Print available functions on console and run the program in a infinite loop
Loop:
	for {
		fmt.Println()
		fmt.Println("0: Exit")
		fmt.Println("1: Execute materialize process")
		fmt.Println("2: Execute materialize microbenchmark")

		// Get user input
		var input int
		fmt.Print("Select a function: ")
		fmt.Scan(&input)

		switch input {
		case 0:
			// Exit programm
			break Loop
		case 1:
			// Call materilaize view function
			materializeView(db)
		case 2:
			// Get user input for number of iterations
			var numberOfIterations int
			fmt.Print("How many iterations?: ")
			fmt.Scan(&numberOfIterations)

			// Catch not suitable numbers
			for numberOfIterations <= 0 {
				fmt.Print("Please input a correct number: ")
				fmt.Scan(&numberOfIterations)
			}

			// Call materializer microbenchmark function with number of iterations
			microbenchmark(db, numberOfIterations)
		default:
			continue
		}
	}

	// Close database, when surrounding fucntion returns
	defer db.Close()

	// Check database with a ping and handle error, if on occured
	err = db.Ping()
	checkError(err)
}

/*
Function to execute the materialize process and write transformed data from event store to materialized view
@param db *sql.DB Database connection to Postgres database
*/
func materializeView(db *sql.DB) {

	// Print information about starting the transformation process
	fmt.Println("Starting materialize process...")

	// Save starting time point
	start := time.Now()

	// Call materialize function with opened database connection
	numberOfMeasurements := materialize(db)

	// Save end time point and calculate difference between start and end time to calculate the materialize process time
	end := time.Now()
	elapsed := end.Sub(start)

	// Print needed time for materializing
	fmt.Printf("Time elapsed: %f seconds for %d measurements\n", elapsed.Seconds(), numberOfMeasurements)
}

/*
Function to control the materialize process
@param db *sql.DB Database connection to Postgres database
*/
func materialize(db *sql.DB) int {
	// Clean materialized view in database
	cleanMaterializedView(db)

	// Read measurements in event store into an array
	measurements := readMeasurements(db)

	// Initialize counter for found measurements
	var counter int

	// Print progress bar of the transforming process
	bar := progressbar.Default(int64(len(measurements)))

	// Iterate through found measurements and transform and write them into the materialized view
	for _, measurement := range measurements {
		// Increment counter for every iterated measurement
		counter++
		// Call transform measurement function with current measurement and database connection
		transformMeasurement(measurement, db)
		// Update the progress bar
		bar.Add(1)
	}

	// Return number of measurements
	return len(measurements)
}

/*
Method to read all measurement from event_store in database and return them as an array
@param db *sql.DB Database connection to Postgres database
@return Array of all read measurements
*/
func readMeasurements(db *sql.DB) []Measurement {

	// Execute select query on event store and return all measurement rows
	rows, err := db.Query("SELECT * FROM event_store ORDER BY id")

	// Check on error with handler
	checkError(err)

	// Close rows object later, when surrounding fucntion returns
	defer rows.Close()

	// Initialize an array for measurements
	measurements := make([]Measurement, 0)

	// Iterate through all records in rows
	for rows.Next() {
		// Initialize empty measurement object
		var measurement Measurement
		// Try to scan a record in row for measurement attributes and set them into the object
		err = rows.Scan(&measurement.id, &measurement.created_on, &measurement.event_stream, &measurement.humidity, &measurement.processed_on, &measurement.sensor_id, &measurement.temperature)
		// Check on error with handler
		checkError(err)
		// Insert measurement into measurements array
		measurements = append(measurements, measurement)
	}

	// Return measurements array
	return measurements
}

/*
Transform a measurement by calculating and setting latency in milliseconds and danger level. Write into database
@param measurement Measurement to be transformed
@param db *sql.DB Database connection to Postgres database
*/
func transformMeasurement(measurement Measurement, db *sql.DB) {

	// Initialize empty transformed measurement object
	var TransformedMeasurement TransformedMeasurement
	// Set base attributes with data from given measurement
	TransformedMeasurement.Measurement = measurement

	// Calculate latency between creation datetime and processed datetime to get it in Nanoseconds then divide it by 1.000.000 to get Milliseconds
	TransformedMeasurement.latency = (float32(int(TransformedMeasurement.processed_on.UnixNano()) - int(TransformedMeasurement.created_on.UnixNano()))) / 1000000

	// Set danger level by traversing through if-statements, that check temperature and humidity
	if TransformedMeasurement.temperature > 10 || TransformedMeasurement.humidity > 60 {
		TransformedMeasurement.danger = Critical
	} else if TransformedMeasurement.temperature > 7 || TransformedMeasurement.humidity > 50 {
		TransformedMeasurement.danger = High
	} else if TransformedMeasurement.temperature > 5 || TransformedMeasurement.humidity > 40 {
		TransformedMeasurement.danger = Medium
	} else if TransformedMeasurement.temperature > 3 || TransformedMeasurement.humidity > 20 {
		TransformedMeasurement.danger = Low
	} else {
		TransformedMeasurement.danger = No
	}

	// Write transformed measurement to materialized view
	writeTransformedMeasurement(TransformedMeasurement, db)
}

/*
Function to persist a transformed measurement in the database
@param TransformedMeasurement Transformed measurement to write into materialized view
@param db *sql.DB Database connection to Postgres database
*/
func writeTransformedMeasurement(TransformedMeasurement TransformedMeasurement, db *sql.DB) {

	// Prepare dynamic insert statement
	insertStmt := "INSERT INTO materialized_view VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	// Initialize error variable
	var err error

	// Execute insert statement with attribute data from the trasformed measurement object
	_, err = db.Exec(insertStmt,
		TransformedMeasurement.id,
		TransformedMeasurement.created_on,
		TransformedMeasurement.danger,
		TransformedMeasurement.event_stream,
		TransformedMeasurement.humidity,
		TransformedMeasurement.latency,
		TransformedMeasurement.processed_on,
		TransformedMeasurement.sensor_id,
		TransformedMeasurement.temperature)

	// Check on error with handler
	checkError(err)
}

/*
Function to clean up the materialized view by deleting all data
@param db *sql.DB Database connection to Postgres database
*/
func cleanMaterializedView(db *sql.DB) {

	// Execute delete statement on database
	_, err := db.Exec("DELETE FROM materialized_view")
	// Check on error with handler
	checkError(err)
}

/*
Handler for possibly found errors
@param err Given error to check
*/
func checkError(err error) {

	// Check if error is not empty
	if err != nil {
		// Panic exception if error is found
		panic(err)
	}
}

// Object structure for a measurement
type Measurement struct {
	// Unique identifier and primary key for a measurement object
	id int64
	// Unique identifier of the sensor, that "measured" the measurement
	sensor_id int64
	// Measured temperature in Grad Celsius (e.g. -1°C)
	temperature float32
	// Measured humidity in percentage (e.g. 12%)
	humidity float32
	// Name of the streaming technology that was used to stream the measurement. For filtering and querying purposes
	event_stream string
	// Date and time with milliseconds as a timestamp on when the measurement was created
	created_on time.Time
	// Date and time with milliseconds as a timestamp on when the measurement was processed by the event stream and event handler (the consumer)
	processed_on time.Time
}

// Object structure for a transformed measurement
type TransformedMeasurement struct {
	// Base data of the measurement
	Measurement
	// Danger level of a measurement and state of the cold storage. Dependent on measured temperature and humidity
	danger string
	// Duration for processing a measurement event between creation timestamp and processing timestamp
	latency float32
}

/*
Function to execute the materialize process several time to measure the performance
@param db *sql.DB Database connection to Postgres database
iterations int Number of iterations
*/
func microbenchmark(db *sql.DB, iterations int) {

	// Print information about starting the test
	fmt.Println("Starting microbenchmark...")

	// Number of processed datapoints
	var numberOfMeasurements int

	// Array list for each iteration duration
	iterationDurations := make([]float64, 0)

	for i := 0; i < iterations; i++ {
		// Save starting time point
		start := time.Now()

		// Call materialize function with opened database connection
		numberOfMeasurements = materialize(db)

		// Save end time point and calculate difference between start and end time to calculate the materialize process time
		end := time.Now()
		elapsed := end.Sub(start)

		// Add duration to array
		iterationDurations = append(iterationDurations, elapsed.Seconds())

		// Print needed time for materializing
		fmt.Printf("Iteration %d/%d finished\n", (i + 1), iterations)
	}

	// Make copy of unordered list
	unorderedIterationDurations := make([]float64, len(iterationDurations))
	copy(unorderedIterationDurations, iterationDurations)

	// Sort iteration durations
	sort.Slice(iterationDurations, func(i, j int) bool {
		return iterationDurations[i] < iterationDurations[j]
	})

	// Calculate average duration
	// Get total of all values
	var sum float64
	for i := 0; i < len(iterationDurations); i++ {
		sum += (iterationDurations[i])
	}
	// Divide total by number of iterations
	var averageDuration float64 = sum / float64(iterations)

	// Calculate median duration
	var medianDuration float64
	// For even iterations
	if iterations%2 == 0 {
		medianDuration = (iterationDurations[iterations/2] + iterationDurations[iterations/2-1]) / 2
		// For odd iterations
	} else {
		medianDuration = (iterationDurations[iterations/2])
	}

	// Calculate variance and standard deviation
	var temp, standardDeviation, variance float64

	// Find sum of square distances to the mean for every duration
	for i := 0; i < len(iterationDurations); i++ {
		temp += math.Pow(iterationDurations[i]-averageDuration, 2)
	}

	// Divide sum by number of iterations to get variance
	variance = temp / float64(len(iterationDurations))
	// Take square root for standard deviation
	standardDeviation = math.Sqrt(variance)

	// Print information about finished test
	fmt.Print("Microbenchmark finished\n\n")

	// Display string with microbenchmark statistics to the console
	fmt.Println("Go Materializer Microbenchmark")
	fmt.Printf("Number of Iterations:\t\t%d\n", iterations)
	fmt.Printf("Datapoints processed each:\t%d\n", numberOfMeasurements)
	fmt.Printf("Fastest iteration (min):\t%f seconds\n", iterationDurations[0])
	fmt.Printf("Slowest iteration (max):\t%f seconds\n", iterationDurations[len(iterationDurations)-1])
	fmt.Printf("Average duration (avg/mean):\t%f seconds\n", averageDuration)
	fmt.Printf("Median duration (median):\t%f seconds\n", medianDuration)
	fmt.Printf("Standard deviation:\t\t%f seconds\n", standardDeviation)
	fmt.Printf("Variance:\t\t\t%f seconds\n\n\n", variance)
	fmt.Println("All runs:")
	fmt.Println(iterationDurations)
	fmt.Println()
	fmt.Println("All runs (unsorted):")
	fmt.Println(unorderedIterationDurations)
	fmt.Println()
}
