# Project Documentation

**Project Overview**

This Node.js project is designed to efficiently count prime numbers from a given input file (defaulting to `input.txt`). It leverages worker threads to perform calculations in parallel, making use of multiple CPU cores to speed up the process. The project also includes features for dynamic task allocation, error handling, and performance monitoring.

**Core Logic and Components**

1.  **`src/main.ts` (Main Thread Orchestration)**
    *   **Initialization:**
        *   Starts a timer to measure total execution time.
        *   Initializes `WorkerStatsManager` to track performance metrics of worker threads.
        *   Initializes `TaskManager` to manage the division of the input file into processable chunks (tasks).
        *   Sets up data structures to manage busy workers, failed tasks, and the main task queue.
    *   **File Handling:**
        *   Determines the input file path (either from a command-line argument or defaulting to `input.txt`).
        *   Reads the file size to calculate progress and divide the file into initial tasks.
    *   **Worker Creation and Management:**
        *   `createWorker`: A function to spawn new worker threads. Each worker is given an initial task.
            *   It initializes statistics for the new worker.
            *   It passes necessary data to the worker (worker ID, file path, initial task).
            *   It sets resource limits for the worker to manage memory usage.
            *   It registers event listeners for messages (`message`) and errors (`error`) from the worker.
    *   **Task Assignment (`assignTaskToWorker`):**
        *   This is a crucial part of the main thread's logic. When a worker becomes free or a new worker is created:
            *   It prioritizes re-assigning tasks that previously failed.
            *   If no failed tasks, it assigns tasks from the main queue. It has a basic strategy to assign smaller tasks to workers identified as "slow" if multiple tasks are available.
            *   **Adaptive Task Creation:** If the initial tasks are exhausted but parts of the file remain unprocessed (`remainingFileRange`), it dynamically creates new tasks. The size of these adaptive tasks is determined by the `TaskManager` based on global average processing time and the specific worker's performance (slow, average, fast).
    *   **Handling Worker Messages (`handleWorkerMessage`):**
        *   When a worker sends a message (indicating task completion):
            *   It aggregates the prime count and bytes processed.
            *   Updates the `WorkerStatsManager` with the worker's performance data for that task.
            *   Updates the `TaskManager` with the processing time to refine future adaptive task sizing.
            *   Updates and displays progress (percentage complete, memory usage, individual worker stats).
            *   Assigns a new task to the now-free worker.
    *   **Error Handling:**
        *   If a worker emits an `error`:
            *   The error is logged.
            *   The task the worker was processing is added to the `failedTasks` queue to be re-processed.
            *   The busy worker is removed from the set.
            *   A new worker is created to replace the failed one, and it's assigned a task (either a failed task, a regular task, or an adaptive task).
    *   **Completion:**
        *   The process ends when all workers are idle, the main task queue is empty, the failed task queue is empty, and the entire file has been processed.
        *   It then prints the final prime count, total execution time, and a summary of worker performance.
    *   **Worker Thread Code (within `main.ts` `else` block):**
        *   If the script is not running in the main thread (i.e., it's a worker thread), it calls `handleTasks` from `src/worker.ts`.

2.  **`src/worker.ts` (Worker Thread Logic)**
    *   **`handleTasks(workerData)`:**
        *   This is the main function executed by each worker thread.
        *   It receives `workerData` (worker ID, file path, initial task).
        *   It enters a loop, processing its `currentTask`.
        *   **`processTask(task, filePath)`:**
            *   This asynchronous function is responsible for processing a single task.
            *   It creates a `ReadableStream` for the specified byte range (`task.startByte` to `task.endByte`) of the input file.
            *   It uses `readline.createInterface` to read the stream line by line.
            *   **Line Processing:**
                *   It handles a potential partial first line if `task.startByte > 0` by skipping it.
                *   For each subsequent line, it parses the number and calls `isPrime` to check for primality.
                *   If prime, it increments a local `count`.
                *   Includes error handling for parsing lines.
            *   Once the stream (chunk) is fully processed (`rl.on('close')`), it resolves a promise with the `count` of primes found in that chunk and the `time` taken to process it.
            *   Includes error handling for file stream creation and reading.
        *   After `processTask` completes, the worker sends a message back to the main thread containing:
            *   `count`: Number of primes found in the task.
            *   `workerId`: Its own ID.
            *   `taskCompleted`: The task object it just finished.
            *   `processingTime`: Time taken for the task.
            *   `memoryUsage`: Its current memory usage.
        *   If `processTask` throws an error:
            *   The worker sends an error message to the main thread with details about the failed task and the error.
            *   It then re-throws the error, causing the worker's 'error' event to fire on the main thread.
        *   **Waiting for Next Task:** After processing a task (or if the initial task is done), the worker waits for a new message from the main thread.
            *   If the message type is `'task'`, it sets this as its `currentTask` and continues the loop.
            *   If the message type is `'exit'`, it resolves the promise, and the `handleTasks` loop terminates, effectively ending the worker.

3.  **`src/primeCalculator.ts` (`isPrime` function)**
    *   This function determines if a given number is prime.
    *   **Basic Checks:** Handles numbers `<= 1`, `2`, `3`, and even/multiples of 3.
    *   **Trial Division:** For numbers less than `TRIAL_DIVISION_THRESHOLD` (currently 10,000):
        *   It uses an optimized trial division method (checking divisibility by `6k ± 1`).
    *   **Miller-Rabin Primality Test:** For larger numbers:
        *   It converts the number to `BigInt` for arbitrary-precision arithmetic.
        *   It performs the Miller-Rabin test, a probabilistic algorithm that is deterministic for numbers within a certain range when specific bases are used.
        *   `powerBigInt`: Helper for modular exponentiation.
        *   `millerTestBigInt`: Implements one iteration of the Miller-Rabin test.
        *   It uses a predefined set of bases (`bases_for_64bit`) that are known to give deterministic results for 64-bit integers.

4.  **`src/taskManager.ts` (`TaskManager` class)**
    *   **Constants:** Defines `MIN_CHUNK_SIZE`, `MAX_CHUNK_SIZE`, and `NUM_CORES` (number of CPU cores).
    *   **`createInitialTasks(fileSize)`:**
        *   Divides the input file into initial chunks (tasks).
        *   `calculateOptimalChunkSize`: Calculates an initial chunk size based on file size and number of cores, aiming for a balance (e.g., trying to create a few tasks per core initially, with constraints on min/max chunk size).
    *   **`calculateChunkSize(globalAverageTime)`:**
        *   This method is used to determine the size for *adaptive* tasks.
        *   It uses the `recentGlobalAverage` processing time (if available) or a provided `globalAverageTime`.
        *   It adjusts the chunk size: smaller if average processing times are high, larger if they are low, within `MIN_CHUNK_SIZE` and `MAX_CHUNK_SIZE` bounds.
    *   **`createAdaptiveTask(startByte, endByte, workerPerformance, globalAverageTime)`:**
        *   Creates a new task dynamically.
        *   It first gets a `baseChunkSize` using `calculateChunkSize`.
        *   It then adjusts this `baseChunkSize` based on the `workerPerformance` ('slow', 'average', 'fast'):
            *   Slow workers get smaller chunks.
            *   Fast workers get larger chunks.
        *   Ensures the chunk size doesn't exceed the remaining data to be processed.
    *   **`addPerformanceData(processingTime)`:**
        *   Maintains a short history of the most recent task processing times.
        *   Updates `recentGlobalAverage` based on this history. This helps in making more responsive adjustments to adaptive task sizes.
    *   **`getNumCores()`:** Returns the number of CPU cores.

5.  **`src/workerStats.ts` (`WorkerStatsManager` class)**
    *   **`workerStats` (Map):** Stores statistics for each worker (tasks completed, total processing time, primes found, average processing time).
    *   **`currentTasks` (Map):** Tracks the task currently being processed by each worker. This is important for re-queueing a task if a worker fails.
    *   **`globalAverageProcessingTime`:** A running average of processing time across all tasks from all workers.
    *   **`initWorkerStats(workerId)`:** Initializes stats for a new worker.
    *   **`updateWorkerStats(result)`:**
        *   Called when a worker completes a task.
        *   Updates the individual worker's stats.
        *   Updates the `globalAverageProcessingTime` efficiently.
        *   Clears the `currentTask` for that worker.
        *   Uses a simple `updateLock` (busy wait) to prevent race conditions when updating shared stats, though this is a very basic form of locking and might not be robust under extreme contention.
    *   **`setCurrentTask(workerId, task)` / `getCurrentTask(workerId)` / `clearWorkerTask(workerId)`:** Manage the `currentTasks` map.
    *   **`getWorkerPerformance(workerId)`:**
        *   Categorizes a worker as 'slow', 'average', or 'fast' by comparing its average processing time to the `globalAverageProcessingTime`.
        *   Requires a few tasks to be completed globally before making a determination.
    *   **`updateProgress(fileSize, totalBytesProcessed, startTime)`:**
        *   Periodically (every `PROGRESS_UPDATE_INTERVAL` milliseconds) clears the console and prints:
            *   Overall progress percentage.
            *   Main thread memory usage.
            *   Statistics for each worker (tasks completed, primes found, avg processing time, performance category, current task if any).
            *   Global average processing time.
            *   Estimated time remaining.
    *   **`printFinalStats()`:** Prints a summary of performance for each worker and the global average at the end.
    *   **`printTimeEstimate(...)`:** Calculates and prints an estimated time remaining.

6.  **`src/types.ts`**
    *   Defines TypeScript interfaces for data structures used throughout the application:
        *   `Task`: Represents a chunk of the file to be processed.
        *   `WorkerInput`: Data passed to a worker when it's created.
        *   `WorkerResult`: Data sent from a worker to the main thread upon task completion.
        *   `WorkerError`: Data sent from a worker to the main thread upon error.
        *   `WorkerStats`: Structure for storing performance statistics of a worker.
        *   `WorkerPerformance`: Type alias for 'slow' | 'average' | 'fast'.

7.  **`input.txt`**
    *   A simple text file containing numbers, one per line, which the program reads to find primes.

**How it Achieves its Task (Prime Counting with Multithreading)**

1.  **Divide and Conquer:** The main thread divides the input file into multiple chunks (tasks).
2.  **Parallel Processing:** It creates a pool of worker threads (typically one per CPU core). Each worker is assigned a task.
3.  **Independent Calculation:** Each worker reads its assigned chunk of the file, processes the numbers in that chunk, and counts the primes independently using the `isPrime` function.
4.  **Communication:**
    *   Workers send results (prime count for their chunk, processing time) back to the main thread.
    *   The main thread sends new tasks to idle workers.
5.  **Aggregation:** The main thread aggregates the prime counts from all workers to get the total.
6.  **Dynamic Task Sizing and Load Balancing:**
    *   The system starts with initial task sizes.
    *   As tasks complete, the `TaskManager` and `WorkerStatsManager` gather performance data.
    *   For remaining parts of the file, *adaptive tasks* are created. The size of these tasks is adjusted based on:
        *   Overall average processing speed.
        *   The performance of the specific worker that will receive the task (slower workers might get smaller adaptive tasks).
    *   Failed tasks are re-queued and prioritized.
7.  **Performance Monitoring:** The `WorkerStatsManager` continuously tracks and displays progress and performance metrics, giving insight into how efficiently the work is being distributed and processed.

**Key Strengths of this Approach**

*   **Parallelism:** Utilizes multiple CPU cores effectively, significantly speeding up computation for large input files compared to a single-threaded approach.
*   **Dynamic Task Management:** The adaptive task creation and assignment strategy attempts to balance the load among workers and adjust to varying processing speeds or complexities in different parts of the file.
*   **Error Resilience (Basic):** Re-queues tasks from failed workers, allowing the process to continue.
*   **Performance Insights:** Provides detailed statistics on worker performance and overall progress.
