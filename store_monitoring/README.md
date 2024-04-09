# Store monitoring
This is an API for store monitoring. It allows users to manage and monitor stores by generating a report of uptime and downtime

## Requirements
Python 3.x
Flask

## Installation
Clone the repository:

git clone --
Change into the project directory:

cd store-monitoring
Install the required dependencies:

Start the server:

flask run

## Usage
### Endpoints
GET /trigger_report: Starts generation of a report of uptime and downtime based on the store timing data

POST /get_report: Get the status of a report, requires report_id in json request body

## Uptime-Downtime Calculation
First we fetch all the store hours and convert them to UTC from local timezone, we only need the time part

We filter out the continuous data to only have timestamp within store hours 

We then go sequentially, calculate the uptime and downtime based on detecting when contiguous blocks on active/inactive switch and keep a running total. 

This calculation is done async and stored in a CSV

## Future Improvements
The data processing could have been done using spark where we could have calculated uptime downtime for all stores parallely.

## License
This project is licensed under the MIT License.