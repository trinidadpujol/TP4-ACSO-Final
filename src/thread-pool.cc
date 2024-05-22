/**
 * File: thread-pool.cc
 * --------------------
 * Presents the implementation of the ThreadPool class.
 */

#include "thread-pool.h"
#include "Semaphore.h"



ThreadPool::ThreadPool(size_t numThreads) : wts(numThreads), schedulingSemaphore(0), workerSemaphore(numThreads), workers(numThreads), done(false) {
    
    // initialize the dispatcher thread
    dt = thread([this]{dispatcher();}); 
        
    // initialize the worker threads
    for(size_t i = 0; i < numThreads; i++)
    {   
        wts[i] = thread([this, i]{worker(i);}); 
    }
}

void ThreadPool::schedule(const function<void(void)>& thunk) {

    // Lock the mutex to add a thunk to the queue (it unlocks when it goes out of scope)
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        tasks.push(thunk);
    }
    // Notify the dispatcher thread that a new task is available
    schedulingSemaphore.signal(); 
}

void ThreadPool::wait() {

    /* Lock the mutex (automatically unlocked while the thread is waiting and re-locked 
    when the wait condition is met or the thread is awakened. */
    std::unique_lock<std::mutex> lock(queueMutex);

    // Wait until there are no more tasks in the queue and if all workers are not working
    threadpoolInactive.wait(lock, [this]{ return tasks.empty() && std::all_of(workers.begin(), workers.end(), [](WorkerData& wd){ return wd.working; }); });
}

ThreadPool::~ThreadPool() {

    wait();
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        done = true;
    }
    schedulingSemaphore.signal(); // Signal the dispatcher thread to exit
    dt.join();              // Join dispatcher thread

    for (size_t i = 0; i < wts.size(); i++) {
        {
            unique_lock<std::mutex> lock(workers[i].mutex);
            workers[i].stop.notify_one(); // notify all worker threads to stop
        }
        wts[i].join(); // join all worker threads
    }
}

void ThreadPool::dispatcher() {
    while (true) {
        std::function<void(void)> task;
        {
            // Lock the mutex to get a task from the queue (it unlocks when it goes out of scope)
            std::unique_lock<std::mutex> lock(queueMutex);
            taskCV.wait(lock, [this]{ return done || !tasks.empty(); });
            if (done && tasks.empty()) break;
            task = std::move(tasks.front());
            tasks.pop();
        }
        workerSemaphore.wait(); // Wait for an available worker
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            // Here we could signal to a worker thread
        }
        // Execute the task
        task();
        workerSemaphore.signal(); // Signal that the worker is done
    }
}

void ThreadPool::worker(size_t workerID) {
    while (true) {
        std::function<void(void)> task;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            taskCV.wait(lock, [this]{ return done || !tasks.empty(); });
            if (done && tasks.empty()) break;
            task = std::move(tasks.front());
            tasks.pop();
        }
        // Execute the task
        task();
        workerSemaphore.signal(); // Signal that the worker is done
    }
}