/**
 * File: thread-pool.cc
 * --------------------
 * Presents the implementation of the ThreadPool class.
 */

#include "thread-pool.h"
#include "Semaphore.h"


ThreadPool::ThreadPool(size_t numThreads) 
    : wts(numThreads), schedulingSemaphore(0), workerSemaphore(numThreads), workers(numThreads), done(false) {
    
    dt = thread([this]{dispatcher();});    // Initialize the dispatcher thread   
    for(size_t i = 0; i < numThreads; i++)    
    {   
        wts[i] = thread([this, i]{worker(i);});    // initialize the worker threads
    }
}

void ThreadPool::schedule(const function<void(void)>& thunk) {
    {
        std::lock_guard<std::mutex> lock(queueMutex);   // Lock the mutex to add a thunk to the queue (it unlocks when it goes out of scope)
        tasks.push(thunk);
    }
    schedulingSemaphore.signal();   // Notify the dispatcher thread that a new task is available
}

void ThreadPool::wait() {
    /* Lock the mutex (automatically unlocked while the thread is waiting and re-locked 
    when the wait condition is met or the thread is awakened. */
    std::unique_lock<std::mutex> lock(queueMutex);

    // Wait until there are no more tasks in the queue and if all workers are not working
    threadpoolInactive.wait(lock, [this]{ return tasks.empty() && std::all_of(workers.begin(), workers.end(), [](WorkerData& wd){ return !wd.working; }); });
}

ThreadPool::~ThreadPool() {

    wait();
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        done = true;
    }
    schedulingSemaphore.signal();     // Signal the dispatcher thread to exit
    dt.join();                        // Join dispatcher thread
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
        schedulingSemaphore.wait();      // Sleeps until a schedule signals it to wake up

        std::function<void(void)> task;
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (done && tasks.empty()) break;              // Exit the loop if done and no tasks are available
            if (!tasks.empty()) {          
                task = tasks.front();       
                tasks.pop();                  
            }
        }

        if (task) {
            workerSemaphore.wait();        // Sleeps until a worker signals it to wake up if all workers are busy   

            for (size_t i = 0; i < workers.size(); ++i) {
                std::unique_lock<std::mutex> lock(workers[i].mutex);
                if (!workers[i].working) {
                    workers[i].working = true;         // Set the worker to working
                    workers[i].task = move(task);     // Pass the task to the worker
                    workers[i].stop.notify_one();     // Notify the worker to start working
                    break;
                }
            }
        }
    }
}

void ThreadPool::worker(size_t workerID) {

    while (true) {
        std::unique_lock<std::mutex> lock(workers[workerID].mutex);
        // Sleep until the dispatcher signals it to wake up or the task is done
        workers[workerID].stop.wait(lock, [this, workerID]{ return done || workers[workerID].working; });

        if (done && !workers[workerID].working) {   // Exit the loop if done and the worker is not working
            break;
        }

        if (workers[workerID].working) {       
            std::function<void(void)> task;       
            {
                std::lock_guard<std::mutex> lock(queueMutex);     
                if (!tasks.empty()) {
                    task = tasks.front();
                    tasks.pop();
                }
            }

            if (task) {     
                task();
                {
                    std::lock_guard<std::mutex> lock(workers[workerID].mutex);
                    workers[workerID].working = false;    // Set the worker to not working
                }
                workerSemaphore.signal();       // Signal the dispatcher that the worker is available         
                threadpoolInactive.notify_all();   // Notify the dispatcher that the task is done
            }
        }
    }
}