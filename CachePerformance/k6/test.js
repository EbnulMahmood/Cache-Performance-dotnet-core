import http from 'k6/http';
import { check, sleep } from 'k6';

//const url = 'https://localhost:44362';
//hazelcast
const url = 'https://localhost:44362/hazelcast/cache';

// Define the endpoints to test
const endpoints = [
    // `${url}/subject-wise-highest-marks/and-exam-count`,
    // `${url}/top-performing-students/by-subject`,
    // `${url}/top-students-by-average-mark/1/true`,
    // `${url}/low-performing-students/by-average-mark/1/true`,
    // `${url}/high-performing-students/by-average-mark/1/true`,
    // `${url}/students-with-lowest-marks/1/true`,
    `${url}/students-with-highest-marks/1/true`
];

// Define the options for the test
export let options = {
    // Simulate 100 virtual users
    vus: 100,
    // Use a constant arrival rate scenario
    scenarios: {
        constant_request_rate: {
            executor: 'constant-arrival-rate',
            rate: 10,
            timeUnit: '1s',
            // Run the test for 10 minutes
            duration: '10m',
            preAllocatedVUs: 100,
            maxVUs: 200
        }
    }
};

// Define the main function for the test
export default function () {
    // Loop through the endpoints and make a GET request to each one
    for (let endpoint of endpoints) {
        let res = http.get(endpoint);
        // Check if the response status is 200 (OK)
        check(res, {
            'status is 200': (r) => r.status === 200
        });
        // Wait for 1 second between requests
        sleep(1);
    }
}
