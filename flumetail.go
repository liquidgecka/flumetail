//   Copyright 2013 Orchestrate, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// Flags

var (
	cachedir = flag.String(
		"cachedir",
		"/var/spool/flumetail",
		"The directory where data is stored about\n"+
		"\teach log file being read.",
	)
	dategroup = flag.Int(
		"dategroup",
		1,
		"The capturing group from 'datere' which stores the\n"+
		"\tactual date string which is parsed using 'format'.",
	)
	datere = flag.String(
		"datere",
		"",
		"A regular expression for matching the date within the"+
		"log line.",
	)
	filename = flag.String(
		"file",
		"",
		"The file name to read log items out of.",
	)
	format = flag.String(
		"format",
		"",
		"The date format string (golang).",
	)
	host = flag.String("host", "", "The host to send the log items to.")
	port = flag.Int("port", 0, "The port on the host to send the logs too.")
)

// If called this will terminate the process after printing an error message
// formatted by the given format string and args.
func die(f string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
	os.Exit(1)
}

// This is a structure used to pass a line, and meta data about the line into
// the processor goroutines.
type data struct {
	Device uint64 `json:"device"`
	End    int64  `json:"last_byte"`
	Inode  uint64 `json:"inode"`
	Line   []byte `json:"-"`
}

// This will return a data structure with the given elements populated.
func newData(line []byte, fi os.FileInfo, end int64) *data {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		die("Wrong type returned from FileInfo.Sys(): %T", fi.Sys())
	}
	return &data{
		Device: uint64(stat.Dev),
		End:    end,
		Inode:  stat.Ino,
		Line:   line,
	}
}

// Returns a map[string]interface{} that can be used when sending data to
// flume. This will eventually get quite smart but for now is nothing more
// than a trivial wrapper around the data.
func makeMap(data *data, re *regexp.Regexp) map[string]interface{} {
	line := string(data.Line)
	output := make(map[string]interface{}, 5)
	output["body"] = line

	// If no regexp is defined then return quickly.
	if re == nil {
		return output
	}

	// Find all matching groups, and if there are not enough them
	// then bail out early since we won't be able to find a date.
	groups := re.FindStringSubmatch(line)
	fmt.Println(groups)
	if len(groups) <= *dategroup {
		fmt.Fprintf(os.Stderr, "Malformed line: %s", line)
		return output
	}

	// Attempt to parse the timestamp.
	ts, err := time.Parse(*format, groups[*dategroup])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Malformed date (%s): %s\n",
			err, groups[*dategroup])
		return output
	}

	output["headers"] = map[string]interface{}{
		"timestamp": ts.Unix(),
	}
	return output
}

// Reads elements from the process channel and attempts to write them to the
// flume server at host:port. This will attempt to use transactions where
// possible. Cache is a file that will be used to store meta information
// about what has been read from the given files.
func processRoutine(host string, port int, process chan *data, cache string) {
	// Compile the date regular expression.
	regex, err := regexp.Compile(*datere)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to compile --datere: %s\n",
			err)
		regex = nil
	}

	url := fmt.Sprintf("http://%s:%d/", host, port)
	mime := "application/json"
	for {
		// Json data that will be sent to the flume server.
		body := make([]map[string]interface{}, 0, 100)

		// Blocking get on the first element.
		data := <-process
		body = append(body, makeMap(data, regex))

		// Non blocking read of at most 24 more elements.
		for i := 0; i < 100; i++ {
			select {
			case data = <-process:
				body = append(body, makeMap(data, regex))
				data = data
			default:
				break
			}
		}

		// Get the json data into byte for so we can send it to flume.
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			die("Error marshaling data for flume.")
		}

		// Loop until a successful request is made.
		for {
			// Perform the actual HTTP POST request.
			resp, err := http.Post(url, mime, bytes.NewBuffer(jsonBytes))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unexpected error from http: %s\n", err)
				time.Sleep(time.Second)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Fprintf(os.Stderr,
					"Non 200 response code from the flume server: %d\n",
					resp.StatusCode)
				time.Sleep(time.Second)
				continue
			}

			// Update the cache.
			mode := os.O_WRONLY | os.O_TRUNC | os.O_CREATE
			perm := os.FileMode(0644)
			fd, err := os.OpenFile(cache, mode, perm)
			if err != nil {
				die("Unknown error opening cache file: %s", err)
			}
			encoder := json.NewEncoder(fd)
			if err := encoder.Encode(data); err != nil {
				die("Error reading cache file: %s", err)
			}
			fd.Close()

			// Success!
			break
		}
	}
}

// Reads all the lines in the given file that start after the given start index,
// writing them into the process channel given. Once done this will return the
// index of the '\n' in the last line read.
func readLines(fd *os.File, start int64, process chan *data) (end int64) {
	if ret, err := fd.Seek(start, 0); err != nil {
		die("Error seeking: %s", err)
	} else if ret != start {
		die("Short seek, expected %d, got %d", start, ret)
	}
	end = start
	var prevLine []byte
	for {
		buffer := make([]byte, 64*1024)
		n, err := fd.Read(buffer)
		// We have to process the error AFTER the bytes read due to the strange
		// way that golang does file IO.
		if n > 0 {
			start := 0
			for i, r := range buffer[0:n] {
				if r != '\n' {
					continue
				}
				// This is a new line break, process the line that came before
				// it, including the prevLine data.
				line := make([]byte, i-start+len(prevLine))
				if prevLine != nil {
					copy(line, prevLine)
				}
				copy(line[len(prevLine):], buffer[start:n])
				fdStat, err := fd.Stat()
				if err != nil {
					die("Unexpected error in stat: %s", err)
				}
				end = end + int64(len(line)+1)
				start = i + 1
				prevLine = nil
				process <- newData(line, fdStat, end)
			}
			if start < n {
				prevLine = append(prevLine, buffer[start:]...)
			}
		}

		// Process the error returned in Read() if any.
		if err != nil {
			if err != io.EOF {
				die("Error reading file: %s", err)
			}
			return end
		}
	}
	panic("NOT REACHED")
}

// Watches a file, reading lines out of it so long as they are available,
// and watiching to see if the inode changes along the way.
func watchFile(filename string, host string, port int) {
	process := make(chan *data, 1000)
	defer close(process)

	// Get the location of the cache file. This is the file that is used to
	// store information about the data in this log file that was read/written
	// successfully.
	sep := string(filepath.Separator)
	cachefile := strings.Join(strings.Split(filename, sep), "_")
	cachefile = filepath.Join(*cachedir, cachefile)

	// Read the file.
	var initial *data
	fd, err := os.Open(cachefile)
	if err == nil {
		decoder := json.NewDecoder(fd)
		if err := decoder.Decode(&initial); err != nil {
			fd.Close()
			die("Error reading cache file: %s", err)
		}
		fd.Close()
	} else if !os.IsNotExist(err) {
		die("Unknown error opening cache file: %s", err)
	}
	if initial == nil {
		initial = &data{}
	}

	// Start the http responder in the background.
	go processRoutine(host, port, process, cachefile)

	// The file can be created, deleted, not exist, etc. As such we have to
	// loop watching the file.
	readFile := func() {
		// Read the cache to get 'end'.
		fd, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				// The file has been deleted or has not yet been created.
				// Sleep for a short period then try again.
				time.Sleep(time.Second)
				return
			}
		}
		defer fd.Close()
		fdStat, err := fd.Stat()
		if err != nil {
			die("Unexpected error: %s", err)
		}
		if sysStat, ok := fdStat.Sys().(*syscall.Stat_t); !ok {
			die("Wrong type returned from FileInfo.Sys(): %T", fdStat.Sys())
		} else if sysStat.Ino != initial.Inode {
			// Its a new file.. we need to restart from the beginning.
			initial = newData(nil, fdStat, 0)
		} else if uint64(sysStat.Dev) != initial.Device {
			// Its on a new drive...
			initial = newData(nil, fdStat, 0)
		}
		end := initial.End

		// Read all the lines in the file starting after the last line we read.
		end = readLines(fd, end, process)

		// Now wait for the file to change.
		for {
			stat, err := os.Stat(filename)
			switch {
			// The file was removed. Read all the remaining lines and
			// then bailout.
			case os.IsNotExist(err):
				readLines(fd, end, process)
				return

			// Some other error.
			case err != nil:
				die("Unknown error in stat: %s", err)

			// The file was rotated. There is a new file but it is not
			// the same as the current file. Read to the end of the file
			// and then return so the new file can be opened.
			case !os.SameFile(stat, fdStat):
				readLines(fd, end, process)
				return

			// The file was truncated. In this case we simply lose all of
			// the data written after we last read. Ideally we should
			// avoid log truncation but sometimes its necessary.
			case stat.Size() < fdStat.Size():
				end = 0
				if n, err := fd.Seek(0, 0); err != nil {
					die("Unable to seek: %s", err)
				} else if n != 0 {
					die("Short seek in %s", filename)
				} else if fdStat, err = fd.Stat(); err != nil {
					die("Unable to stat: %s", err)
				}

			// New data was written to the file, we need to read it.
			case stat.Size() > fdStat.Size():
				end = readLines(fd, end, process)
				if fdStat, err = fd.Stat(); err != nil {
					die("Unable to stat: %s", err)
				}

			// The file didn't change in any meaningful way.
			default:
				time.Sleep(time.Second)
			}
		}
	}

	for {
		readFile()
	}
}

func main() {
	// Verify the flags.
	flag.Parse()
	switch {
	case *filename == "":
		die("You must provide a file name to log.")
	case *host == "":
		die("You must provide a host to send logs to.")
	case *port == 0:
		die("You must provide a port to send the logs to.")
	}

	watchFile(*filename, *host, *port)
}
