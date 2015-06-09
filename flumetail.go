//   Copyright 2015 Orchestrate, Inc.
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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Flags

var (
	configDir = flag.String(
		"configdir",
		"/etc/flumetail.d",
		"The directory to load flumetail configs from.",
	)
	cacheDir = flag.String(
		"cachedir",
		"/var/spool/flumetail",
		"The directory where data is stored about each log file being read.",
	)
	caRootDir = flag.String(
		"caroot",
		"/etc/ssl/certs",
		"The directory to load ca roots from. All roots should be named *.pem",
	)

	// If this is ever set to true then the process is terminating so the
	// threads should terminate slowly.
	stopping = false

	// The http client that will be used to initiate connections.
	client = &http.Client{}
)

// This is the configuration type that is used for reading/loading configs
// from disk. Each file contains json matching this object.
type Config struct {
	FileName  string `json:"file_name"`
	URL       string `json:"url"`
	ClientKey string `json:"client_ssl_key"`
	ClientCA  string `json:"client_ssl_ca"`
}

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
func makeMap(data *data) map[string]interface{} {
	return map[string]interface{}{
		"body": string(data.Line),
	}
}

// Reads elements from the process channel and attempts to write them to the
// flume server. This will attempt to use transactions where possible. Cache
// is a file that will be used to store meta information about what has been
// read from the given files.
func processRoutine(config *Config, process chan *data, cache string) {
	mime := "application/json"
	for !stopping {
		// Json data that will be sent to the flume server.
		body := make([]map[string]interface{}, 0, 100)

		// Blocking get on the first element.
		data := <-process
		body = append(body, makeMap(data))

		// Non blocking read of at most 24 more elements.
		for i := 0; i < 100; i++ {
			select {
			case data = <-process:
				body = append(body, makeMap(data))
			default:
				break
			}
		}

		// Get the json data into byte for so we can send it to flume.
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			die("Error marshaling data for flume.")
		}

		// Loop until a successful request is made or the process is in
		// stopping mode.
		for !stopping {
			// Perform the actual HTTP POST request.
			resp, err := client.Post(
				config.URL, mime, bytes.NewBuffer(jsonBytes))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unexpected error from http: %s\n", err)
				time.Sleep(time.Second)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Fprintf(os.Stderr,
					"Non 200 response code from the flume server at %s: %d\n",
					config.URL, resp.StatusCode)
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
	for !stopping {
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

	return end
}

// Watches a file, reading lines out of it so long as they are available,
// and watching to see if the inode changes along the way.
func watchFile(config *Config, wg *sync.WaitGroup, lock *sync.Mutex) {
	lock.Lock()
	defer wg.Done()
	process := make(chan *data, 1000)
	defer close(process)

	// Get the location of the cache file. This is the file that is used to
	// store information about the data in this log file that was read/written
	// successfully.
	sep := string(filepath.Separator)
	cachefile := strings.Join(strings.Split(config.FileName, sep), "_")
	cachefile = filepath.Join(*cacheDir, cachefile)

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
	go processRoutine(config, process, cachefile)

	// The file can be created, deleted, not exist, etc. As such we have to
	// loop watching the file.
	readFile := func() {
		// Read the cache to get 'end'.
		fd, err := os.Open(config.FileName)
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
		for !stopping {
			stat, err := os.Stat(config.FileName)
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
					die("Short seek in %s", config.FileName)
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

	for !stopping {
		readFile()
	}
}

// Loads CA roots into the HTTP client. On error this just terminates the
// process. This will run in the background after all of the goroutines have
// started in order to make https work.
func loadCARoots(clientCerts map[string]string, lock *sync.Mutex) {
	defer lock.Unlock()

	// Create a pool to store all of the roots.
	caPool := x509.NewCertPool()

	caFiles, err := ioutil.ReadDir(*caRootDir)
	if err != nil {
		die("Can not read caroot directory (%s): %s", *caRootDir, err)
	}
	for _, file := range caFiles {
		if filepath.Ext(file.Name()) != ".pem" {
			continue
		}
		fn := filepath.Join(*caRootDir, file.Name())
		data, err := ioutil.ReadFile(fn)
		if err != nil {
			die("Can not load ca file (%s): %s", fn, err)
		}
		if !caPool.AppendCertsFromPEM(data) {
			fmt.Printf("Unable to load PEM file (%s).\n", fn)
		}
	}

	// Setup HTTPS client object.
	tlsConfig := &tls.Config{
		RootCAs: caPool,
	}

	// Next walk through setting up each of the client certificates.
	for key, cert := range clientCerts {
		certData, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			die("Error loading (%s) certificate: %s", cert, key)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, certData)
	}

	// Setup the client
	tlsConfig.BuildNameToCertificate()
	client.Transport = &http.Transport{TLSClientConfig: tlsConfig}
}

func main() {
	// Verify the flags.
	flag.Parse()
	switch {
	case *cacheDir == "":
		die("You must provide a cache dir for data.")
	case *configDir == "":
		die("You must provide a config dir for data.")
	}

	// Setup a SIGTERM handler.
	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGTERM)
	go func() {
		for sig := range sigChan {
			fmt.Printf("%s recieved, shutting down.", sig)
			stopping = true
		}
	}()

	// This lock is used to ensure that workers do not actually start until
	// the TLS configuration has loaded.
	lock := sync.Mutex{}
	lock.Lock()

	// Stores the map of any client certificates that are necessary.
	clientCerts := make(map[string]string, 0)

	// Read the configs.
	files, err := ioutil.ReadDir(*configDir)
	if err != nil {
		die("Error listing config files: %s", err)
	}
	wg := sync.WaitGroup{}
	found := 0
	for _, file := range files {
		fn := filepath.Join(*configDir, file.Name())
		if filepath.Ext(file.Name()) != ".conf" {
			continue
		}
		body, err := ioutil.ReadFile(fn)
		if err != nil {
			die("Unable to read config file %s: %s", file.Name(), err)
		}
		config := &Config{}
		if err := json.Unmarshal(body, config); err != nil {
			die("Error loading config file %s: %s", file.Name(), err)
		}
		wg.Add(1)
		go watchFile(config, &wg, &lock)
		found += 1

		// See if we need to load a key.
		if config.ClientKey != "" || config.ClientCA != "" {
			if config.ClientKey == "" {
				die("client_ssl_key is empty in %s", fn)
			} else if config.ClientCA == "" {
				die("client_ssl_ca is empty in %s", fn)
			} else if ca, ok := clientCerts[config.ClientKey]; ok {
				if ca != config.ClientCA {
					die("client_ssl_ca is different for %s", fn)
				}
				// Do nothing.. Its already set.
			} else {
				clientCerts[config.ClientKey] = config.ClientCA
			}
		}
	}

	if found == 0 {
		fmt.Printf("No configuration files found.\n")
	}

	// Load the SSL configs. We have to do this later since we don't have the
	// client cert configs until this point.
	loadCARoots(clientCerts, &lock)

	// Wait for the workers to terminate.
	wg.Wait()

	// Stop the signal handler and shut down.
	signal.Stop(sigChan)
	close(sigChan)
}
