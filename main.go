package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
)

const bunnyDateFormat = "01-02-06"

var (
	token       string
	pullZoneId  int
	startOffset int = 0
	batchSize   int = 2_000
	date            = cli.NewTimestamp(time.Now())
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{}
	app.Commands = []*cli.Command{{
		Name: "download",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "token",
				EnvVars:     []string{"BUNNY_API_TOKEN"},
				Destination: &token,
				Required:    true,
			},
			&cli.IntFlag{
				Name:        "pull-zone-id",
				EnvVars:     []string{"BUNNY_PULL_ZONE_ID"},
				Destination: &pullZoneId,
				Required:    true,
			},
			&cli.IntFlag{
				Name:        "start-offset",
				EnvVars:     []string{"BUNNY_START_OFFSET"},
				Value:       batchSize,
				Destination: &batchSize,
			},
			&cli.IntFlag{
				Name:        "batch-size",
				EnvVars:     []string{"BUNNY_BATCH_SIZE"},
				Value:       batchSize,
				Destination: &batchSize,
			},
			&cli.TimestampFlag{
				Name:        "date",
				EnvVars:     []string{"BUNNY_DOWNLOAD_DATE"},
				Value:       date,
				Destination: date,
				Layout:      "2006-01-02",
			},
		},
		Action: func(ctx *cli.Context) error {
			outputFolder := strconv.Itoa(pullZoneId)
			outputFileName := fmt.Sprintf("%s.log", date.Value().Format("2006-01-02"))
			if err := os.MkdirAll(strconv.Itoa(pullZoneId), os.ModePerm); err != nil {
				return fmt.Errorf("create output folder: %w", err)
			}
			outputFilePath := path.Join(outputFolder, outputFileName)
			out, err := os.Create(outputFilePath)
			if err != nil {
				return fmt.Errorf("create output file: %w", err)
			}
			if err = out.Close(); err != nil {
				return fmt.Errorf("write output file: %w", err)
			}

			start := startOffset
			moreToDownload := true

			for moreToDownload {
				moreToDownload, err = downloadBatch(start*batchSize, outputFilePath)
				if err != nil {
					return fmt.Errorf("download batch with start %v: %w", start, err)
				}
				if start%10 == 0 {
					fmt.Printf("Downloaded batch %v\n", start)
				}
				start += 1
			}
			fmt.Println("Downloaded all logs")

			return nil
		},
	}}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func downloadBatch(start int, outputFilePath string) (bool, error) {
	url := fmt.Sprintf("https://logging.bunnycdn.com/%s/%v.log?sort=desc&start=%v&end=%v", date.Value().Format(bunnyDateFormat), pullZoneId, start, start+batchSize)

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}
	request.Header.Add("AccessKey", token)
	request.Header.Add("Accept-Encoding", "gzip")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return false, fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNoContent {
		return false, nil
	}

	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, fmt.Errorf("create gzip reader: %w", err)
	}

	out, err := os.OpenFile(outputFilePath, os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return false, fmt.Errorf("open output file: %w", err)
	}

	written, err := io.Copy(out, reader)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, fmt.Errorf("read: %w", err)
	}
	if err = out.Close(); err != nil {
		return false, fmt.Errorf("close output file: %w", err)
	}
	return written > 0, nil
}
