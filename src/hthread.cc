#include "hthread.h"
using namespace hikv;
using namespace std;

ThreadPool::ThreadPool() {
    this->type = 0;
    this->num_threads = DEFAULT_NUM_THREADS;
    this->num_queues = DEFAULT_NUM_QUEUES;
    this->queue_size = DEFAULT_QUEUE_SIZE;
}

ThreadPool::ThreadPool(uint8_t type, uint32_t num_threads, uint32_t num_queues, \
                        uint32_t queue_size) {
    this->type = type;
    this->num_threads = num_threads;
    this->num_queues = num_queues;
    this->queue_size = queue_size;
}

struct request_queue *ThreadPool::get_queue(int qid) {
    return this->queues[qid];
}

static hash_table_worker *get_ht_worker(struct request_queue *queue,  int pos) {
    uint32_t p = (pos % queue->size) * sizeof(struct hash_table_worker);
    return (hash_table_worker*)(queue->data + p);
}

static bool queue_is_full(struct request_queue *queue) {
    if(queue->tail + 1 == queue->front)
        return true;
    else 
        return false;
}

static bool queue_is_empty(struct request_queue *queue) {
    if(queue->front == queue->tail)
        return true;
    else 
        return false;
}

// Function to process request.
static void process_request(struct thread_args* args) {
    // Start to process request.
    while(true) {
        for(int i = args->start_qid; i <= args->end_qid; i++) {
            struct request_queue *q_tmp = args->tp->get_queue(i);
            if( queue_is_empty(q_tmp) ) {
                continue;
            }
            // if is HashTable operator.
            struct hash_table_worker *worker = get_ht_worker(q_tmp, q_tmp->front);
            cout << q_tmp->front << " " << q_tmp->tail << endl;
            cout << worker->key << " " << worker->value << endl;
            __sync_add_and_fetch(&(q_tmp->front), 1);
            worker->status->set_running(false);
            // if is bplus tree operator.
        }
    }
}

// Function to start thread.
static void* thread_run(void *args_) {
    struct thread_args *args = (struct thread_args *)args_;
    // TODO thread bind.
    // Here to process request.
    process_request(args);
    return NULL;
}


// Function to init queue, include malloc queue space.
bool ThreadPool::init_queue() {
    uint32_t malloc_size;
    // head (to record info) + body (to save request)
    if (type == TYPE_THREAD_POOL_BTREE) {
        malloc_size = sizeof(request_queue) + \
                queue_size * sizeof(bplus_tree_worker);
    } else if (type == TYPE_THREAD_POOL_HASH) {
        malloc_size = sizeof(request_queue) + \
                queue_size * sizeof(hash_table_worker);
    } else {
        return false;
    }
    for(int i = 0; i < num_queues; i++) {
        queues[i] = (struct request_queue*)malloc(malloc_size);
        // ensure malloc fail
        assert(queues[i] != 0);
        // memset queue space
        memset(queues[i], 0 , malloc_size);
        queues[i]->size = queue_size;
        // if front == tail, queue is empty.
        // if (tail + 1) % size == front, queue is full.
        queues[i]->front = queues[i]->tail = 0;
        queues[i]->weight = 0;
    }
    return true;
}

// Function to init thread.
bool ThreadPool::init_thread() {
    // A process is responsible for multiple queues.
    if (num_queues >= num_threads) {
        this->one_queue_one_consumer = true;
        uint32_t span = num_queues / num_threads;
        for(int i = 0, start_qid = 0; i < num_threads; i++, start_qid += span) {
            struct thread_args *args = new thread_args();
            args->start_qid = start_qid;
            args->end_qid = start_qid + span - 1;
            args->tp = this;
            if (args->end_qid >= num_queues) {
                args->end_qid = num_queues - 1;
            }
            // Here to create threads.
            pthread_create(pthread_id + i, NULL, thread_run, args);
        }
    // All queues are shared by all threads.
    } else {
        this->one_queue_one_consumer = false;
        for(int i = 0; i < num_threads; i++) {
            struct thread_args *args = new thread_args();
            args->start_qid = 0, args->end_qid = num_queues - 1;
            args->tp = this;
            int res = pthread_create(pthread_id + i, NULL, thread_run, args); 
            // res == 0 means create thread succeed. 
            assert(res == 0);
        }
    }
    return true;
}

bool ThreadPool::init() {
    bool res;
    // init queue
    res = init_queue();
    assert(res == true);
    // init thread
    res = init_thread();
    assert(res == true);
    return true;
}

// Function to insert a worker into queue.
bool ThreadPool::add_worker(uint32_t qid, void *object, Slice &key, Slice &value, Status *status) {
    if (this->type == TYPE_THREAD_POOL_BTREE) {
        // TODO bplus tree operator.
    } else if (this->type == TYPE_THREAD_POOL_HASH) {
        if(this->one_queue_one_consumer == true) {
            struct request_queue *q_tmp = this->queues[qid];
            while(true) {
                int front = q_tmp->front;
                int tail = q_tmp->tail;
                if( queue_is_full(q_tmp) ) {
                    // Queue if full.
                    continue;
                }
                struct hash_table_worker *worker = get_ht_worker(q_tmp, tail);
                worker->hash_table = (HashTable *) object;
                worker->key = key, worker->value = value;
                worker->status = status;
                // Atomic operation tail++.
                __sync_add_and_fetch(&(q_tmp->tail), 1);
                break;
            }
        } else {

        }
    } else {
        return false;
    }
    return true;
}

