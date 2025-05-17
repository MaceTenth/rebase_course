# Prime Number Counter

This project reads numbers from an input file (`input.txt`), counts how many of those numbers are prime, and reports the total count along with the time taken for the computation.

It utilizes Node.js worker threads to perform the primality tests in parallel, making use of multiple CPU cores for better performance.

## Setup

1.  Ensure you have Node.js installed (which includes npm).
2.  Clone the repository (if applicable) or download the files.
3.  Navigate to the project directory in your terminal.
4.  Install dependencies: `npm install`

## Running the Project

1.  **Build:** `npm run build` (This compiles the TypeScript code to JavaScript in the `dist` directory)
2.  **Run:** `npm start` (This executes the compiled code)

The script will read numbers from `input.txt` located in the root of the project. Each number should be on a new line.

## Output

The program will print:
- The number of CPU cores utilized.
- The total count of prime numbers found in the input file.
- The time taken for the computation in milliseconds.