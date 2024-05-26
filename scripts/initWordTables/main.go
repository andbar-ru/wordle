package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"

	_ "github.com/lib/pq"
)

type Db struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Dbname   string `json:"dbname"`
}

type Config struct {
	Db Db `json:"db"`
}

const letters = "абвгдежзийклмнопрстуфхцчшщъыьэюя"

var (
	config Config
	db     *sql.DB

	words4        = make(map[string][4]rune)
	words5        = make(map[string][5]rune)
	words6        = make(map[string][6]rune)
	words7        = make(map[string][7]rune)
	letterCount4  = make(map[rune]int, 32)
	letterCount5  = make(map[rune]int, 32)
	letterCount6  = make(map[rune]int, 32)
	letterCount7  = make(map[rune]int, 32)
	letterRating4 = make(map[rune]float64)
	letterRating5 = make(map[rune]float64)
	letterRating6 = make(map[rune]float64)
	letterRating7 = make(map[rune]float64)
	word4Rgx      = regexp.MustCompile(`^[а-яё]{4}$`)
	word5Rgx      = regexp.MustCompile(`^[а-яё]{5}$`)
	word6Rgx      = regexp.MustCompile(`^[а-яё]{6}$`)
	word7Rgx      = regexp.MustCompile(`^[а-яё]{7}$`)
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func initConfig() {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("Could not get the path of executing script")
	}
	configPath := path.Join(path.Dir(filename), "../../config.json")
	file, err := os.Open(configPath)
	checkErr(err)
	defer file.Close()

	data, err := io.ReadAll(file)
	checkErr(err)
	err = json.Unmarshal(data, &config)
	checkErr(err)
}

func getFiles() []*os.File {
	if len(os.Args) < 2 {
		panic("You must specify one or more files containing words!")
	}
	files := make([]*os.File, 0, len(os.Args)-1)
	var err error
	for _, filepath := range os.Args[1:] {
		file, e := os.Open(filepath)
		if e != nil {
			err = fmt.Errorf("Could not open file %s: %v", filepath, e)
			break
		}
		b := make([]byte, 1)
		_, e = file.Read(b)
		if e != nil {
			err = fmt.Errorf("%s is not readable or empty: %v", filepath, e)
			break
		}
		files = append(files, file)
	}
	if err != nil {
		for _, file := range files {
			file.Close()
		}
		panic(err)
	}
	return files
}

func initLetterCounts() {
	for _, letter := range letters {
		letterCount4[letter] = 0
		letterCount5[letter] = 0
		letterCount6[letter] = 0
		letterCount7[letter] = 0
	}
}

func scanFile(file *os.File) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		word := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(scanner.Text())), "ё", "е")
		if word4Rgx.MatchString(word) {
			if _, ok := words4[word]; !ok {
				var letters [4]rune
				var i int
				for _, letter := range word {
					letterCount4[letter]++
					letters[i] = letter
					i++
				}
				words4[word] = letters
			}
		} else if word5Rgx.MatchString(word) {
			if _, ok := words5[word]; !ok {
				var letters [5]rune
				var i int
				for _, letter := range word {
					letterCount5[letter]++
					letters[i] = letter
					i++
				}
				words5[word] = letters
			}
		} else if word6Rgx.MatchString(word) {
			if _, ok := words6[word]; !ok {
				var letters [6]rune
				var i int
				for _, letter := range word {
					letterCount6[letter]++
					letters[i] = letter
					i++
				}
				words6[word] = letters
			}
		} else if word7Rgx.MatchString(word) {
			if _, ok := words7[word]; !ok {
				var letters [7]rune
				var i int
				for _, letter := range word {
					letterCount7[letter]++
					letters[i] = letter
					i++
				}
				words7[word] = letters
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file:", err)
	}

	for letter, count := range letterCount4 {
		rating := math.RoundToEven(float64(count)/float64(len(words4)*4)*1000) / 1000
		letterRating4[letter] = rating
	}
	for letter, count := range letterCount5 {
		rating := math.RoundToEven(float64(count)/float64(len(words5)*5)*1000) / 1000
		letterRating5[letter] = rating
	}
	for letter, count := range letterCount6 {
		rating := math.RoundToEven(float64(count)/float64(len(words6)*6)*1000) / 1000
		letterRating6[letter] = rating
	}
	for letter, count := range letterCount7 {
		rating := math.RoundToEven(float64(count)/float64(len(words7)*7)*1000) / 1000
		letterRating7[letter] = rating
	}
}

func getScore(letters []rune, letterRating map[rune]float64) float64 {
	l2r := make(map[rune]float64, len(letters))
	for _, letter := range letters {
		l2r[letter] = letterRating[letter]
	}
	var score float64
	for _, r := range l2r {
		score += r
	}
	return score
}

func processDb() {
	tx, err := db.Begin()
	checkErr(err)
	defer tx.Rollback()

	_, err = tx.Exec("DROP TABLE IF EXISTS words4, words5, words6, words7")
	checkErr(err)

	_, err = tx.Exec(`CREATE TABLE words4 (
		word CHAR(4) NOT NULL PRIMARY KEY,
		l1 CHAR(1) NOT NULL,
		l2 CHAR(1) NOT NULL,
		l3 CHAR(1) NOT NULL,
		l4 CHAR(1) NOT NULL,
		score NUMERIC(3, 3)
	)`)
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w4l1 ON words4 (l1)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w4l2 ON words4 (l2)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w4l3 ON words4 (l3)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w4l4 ON words4 (l4)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w4score ON words4 (score)")
	checkErr(err)

	_, err = tx.Exec(`CREATE TABLE words5 (
		word CHAR(5) NOT NULL PRIMARY KEY,
		l1 CHAR(1) NOT NULL,
		l2 CHAR(1) NOT NULL,
		l3 CHAR(1) NOT NULL,
		l4 CHAR(1) NOT NULL,
		l5 CHAR(1) NOT NULL,
		score NUMERIC(3, 3)
	)`)
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w5l1 ON words5 (l1)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w5l2 ON words5 (l2)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w5l3 ON words5 (l3)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w5l4 ON words5 (l4)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w5l5 ON words5 (l5)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w5score ON words5 (score)")
	checkErr(err)

	_, err = tx.Exec(`CREATE TABLE words6 (
		word CHAR(6) NOT NULL PRIMARY KEY,
		l1 CHAR(1) NOT NULL,
		l2 CHAR(1) NOT NULL,
		l3 CHAR(1) NOT NULL,
		l4 CHAR(1) NOT NULL,
		l5 CHAR(1) NOT NULL,
		l6 CHAR(1) NOT NULL,
		score NUMERIC(3, 3)
	)`)
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6l1 ON words6 (l1)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6l2 ON words6 (l2)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6l3 ON words6 (l3)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6l4 ON words6 (l4)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6l5 ON words6 (l5)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6l6 ON words6 (l6)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w6score ON words6 (score)")
	checkErr(err)

	_, err = tx.Exec(`CREATE TABLE words7 (
		word CHAR(7) NOT NULL PRIMARY KEY,
		l1 CHAR(1) NOT NULL,
		l2 CHAR(1) NOT NULL,
		l3 CHAR(1) NOT NULL,
		l4 CHAR(1) NOT NULL,
		l5 CHAR(1) NOT NULL,
		l6 CHAR(1) NOT NULL,
		l7 CHAR(1) NOT NULL,
		score NUMERIC(3, 3)
	)`)
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l1 ON words7 (l1)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l2 ON words7 (l2)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l3 ON words7 (l3)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l4 ON words7 (l4)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l5 ON words7 (l5)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l6 ON words7 (l6)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7l7 ON words7 (l7)")
	checkErr(err)
	_, err = tx.Exec("CREATE INDEX w7score ON words7 (score)")
	checkErr(err)

	stmt4, err := tx.Prepare("INSERT INTO words4 VALUES ($1, $2, $3, $4, $5, $6)")
	checkErr(err)
	defer stmt4.Close()
	for word, letters := range words4 {
		score := getScore(letters[:], letterRating4)
		_, err := stmt4.Exec(word, string(letters[0]), string(letters[1]), string(letters[2]), string(letters[3]), score)
		checkErr(err)
	}

	stmt5, err := tx.Prepare("INSERT INTO words5 VALUES ($1, $2, $3, $4, $5, $6, $7)")
	checkErr(err)
	defer stmt5.Close()
	for word, letters := range words5 {
		score := getScore(letters[:], letterRating5)
		_, err := stmt5.Exec(word, string(letters[0]), string(letters[1]), string(letters[2]), string(letters[3]), string(letters[4]), score)
		checkErr(err)
	}

	stmt6, err := tx.Prepare("INSERT INTO words6 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
	checkErr(err)
	defer stmt6.Close()
	for word, letters := range words6 {
		score := getScore(letters[:], letterRating6)
		_, err := stmt6.Exec(word, string(letters[0]), string(letters[1]), string(letters[2]), string(letters[3]), string(letters[4]), string(letters[5]), score)
		checkErr(err)
	}

	stmt7, err := tx.Prepare("INSERT INTO words7 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
	checkErr(err)
	defer stmt7.Close()
	for word, letters := range words7 {
		score := getScore(letters[:], letterRating7)
		_, err := stmt7.Exec(word, string(letters[0]), string(letters[1]), string(letters[2]), string(letters[3]), string(letters[4]), string(letters[5]), string(letters[6]), score)
		checkErr(err)
	}

	_, err = tx.Exec("DROP TABLE IF EXISTS letter_ratings")
	checkErr(err)

	_, err = tx.Exec(`CREATE TABLE letter_ratings (
		letter CHAR(1) NOT NULL PRIMARY KEY,
		words4 NUMERIC(3, 3) NOT NULL,
		words5 NUMERIC(3, 3) NOT NULL,
		words6 NUMERIC(3, 3) NOT NULL,
		words7 NUMERIC(3, 3) NOT NULL
	)`)
	checkErr(err)

	stmt, err := tx.Prepare("INSERT INTO letter_ratings VALUES ($1, $2, $3, $4, $5)")
	checkErr(err)
	defer stmt.Close()
	for _, letter := range letters {
		rating4 := letterRating4[letter]
		rating5 := letterRating5[letter]
		rating6 := letterRating6[letter]
		rating7 := letterRating7[letter]
		_, err := stmt.Exec(string(letter), rating4, rating5, rating6, rating7)
		checkErr(err)
	}

	err = tx.Commit()
	checkErr(err)
}

func main() {
	initConfig()
	files := getFiles()
	for _, file := range files {
		defer file.Close()
	}

	initLetterCounts()
	for _, file := range files {
		scanFile(file)
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		config.Db.Host, config.Db.Port, config.Db.User, config.Db.Password, config.Db.Dbname)
	var err error
	db, err = sql.Open("postgres", psqlInfo)
	checkErr(err)
	defer db.Close()

	processDb()
	fmt.Println("Tables created")
}
