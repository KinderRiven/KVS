#include "hthread.h"
using namespace hikv;
using namespace std;

ThreadPool::ThreadPool() 
{
    this->type = 0;
    this->num_consumers = DEFAULT_NUM_THREADS;
    this->num_queues = DEFAULT_NUM_QUEUES;
    this->queue_size = DEFAULT_QUEUE_SIZE;
}

ThreadPool::ThreadPool(uint8_t type, uint32_t num_producers, \
                        uint32_t num_consumers, uint32_t num_queues, uint32_t queue_size) 
{
    this->type = type;
    this->num_producers = num_producers;
    this->num_consumers = num_consumers;
    this->num_queues = num_queues;
    this->queue_size = queue_size;
}

void ThreadPool::Print()
{
    printf("[ThreadPool]:\n");
    printf("[1] num producers : %2d, [2] num_consumers : %2d\n", this->num_producers, this->num_consumers);
    printf("[3] num queues    : %2d, [4] queue size    : %2d\n", this->num_queues, this->queue_size);
    printf("[5] one queue one consumers : ");
    this->one_queue_one_consumer == true ? printf("true\n") : printf("false\n");
    printf("[6] one queue one producer  : ");
    this->one_queue_one_producer == true ? printf("true\n") : printf("false\n");
}

ThreadPool::~ThreadPool() 
{
    for(int i = 0; i < num_queues; i++) 
    {
        free(queues[i]);
    }
}

struct request_queue *ThreadPool::get_queue(int qid) 
{
    return this->queues[qid];
}

static bplus_tree_worker *get_bplus_worker(struct request_queue *queue,  int pos) 
{
    uint32_t p = (pos % queue->size) * sizeof(struct bplus_tree_worker);
    return (bplus_tree_worker*)(queue->data + p);
}

static uint32_t index_to_pos(struct request_queue *queue, uint32_t index) 
{
    return index % queue->size;
}
 
static bool queue_is_empty(struct request_queue *queue) 
{
    if(queue->read_index == queue->max_read_index) {
        return true;
    } else {
        return false;
    }
}

static void process_bplus_request(struct bplus_tree_worker *worker) 
{
    // Do work
    BpTree *bp = worker->bp;
    bool res = bp->Put(worker->key, worker->data);
    assert(res == true);
}

// Function to process request.
static void process_request(struct thread_args* args) 
{
    // Start to process request.
    while(true) 
    {
        Status tmp_status;
        for(int i = args->start_qid; i <= args->end_qid; i++) 
        {
            struct request_queue *q_tmp = args->tp->get_queue(i);
            if(args->one_queue_one_consumer) {
                // current we can read
                uint32_t read_index = q_tmp->read_index;
                if( queue_is_empty(q_tmp) ) {
                    continue;
                }
                q_tmp->read_index++;
                // here to solve bplus work.
                if(args->tp->get_type() == TYPE_THREAD_POOL_BTREE) {
                    struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, read_index);
                    process_bplus_request(worker);
                }
                __sync_add_and_fetch(&(q_tmp->max_write_index), 1);
            }
            else {
                bool is_empty = false;
                uint32_t read_index, max_read_index;
                do {
                    read_index = q_tmp->read_index;
                    max_read_index = q_tmp->max_read_index;
                    if(read_index == max_read_index) {
                        is_empty = true;
                        break;
                    }
                } while(!__sync_bool_compare_and_swap(&q_tmp->read_index, read_index, read_index + 1));
                // if is empty.
                if(is_empty) {
                    continue;
                }
                if(args->tp->get_type() == TYPE_THREAD_POOL_BTREE) {
                    struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, read_index);
                    process_bplus_request(worker);
                }
                while(!__sync_bool_compare_and_swap(&q_tmp->max_write_index, \
                        read_index, read_index + 1)) {
                    sched_yield();
                }
            }
        }
    }
}

// Function to start thread.
static void* thread_run(void *args_) 
{
    struct thread_args *args = (struct thread_args *)args_;
    // TODO thread bind.
    // Here to process request.
    process_request(args);
    return NULL;
}

// Function to init queue, include malloc queue space.
bool ThreadPool::init_queue() 
{
    uint32_t malloc_size;
    // head (to record info) + body (to save request)
    if (type == TYPE_THREAD_POOL_BTREE) {
        malloc_size = sizeof(request_queue) + \
                queue_size * sizeof(bplus_tree_worker);
    } else {
        return false;
    }
    for(int i = 0; i < num_queues; i++) 
    {
        queues[i] = (struct request_queue*)malloc(malloc_size);
        // ensure malloc fail
        assert(queues[i] != 0);
        // memset queue space
        memset(queues[i], 0 , malloc_size);
        queues[i]->size = queue_size;
        // if read_index == max_read_index, queue is empty.
        queues[i]->read_index = queues[i]->max_read_index = 0;
        queues[i]->write_index = queues[i]->max_write_index = 0;
        queues[i]->weight = 0;
    }
    return true;
}

// Function to init thread.
bool ThreadPool::init_thread() 
{
    // A process is responsible for multiple queues.
    if (num_queues >= num_consumers) 
    {
        this->one_queue_one_consumer = true;
        uint32_t span = num_queues % num_consumers == 0 ? \
            num_queues / num_consumers : num_queues / num_consumers + 1;
        for(int i = 0, start_qid = 0; i < num_consumers; i++, start_qid += span) 
        {
            struct thread_args *args = new thread_args();
            // thread should to scan the queue[start, end]
            args->start_qid = start_qid;
            args->end_qid = start_qid + span - 1;
            // threadpool
            args->tp = this;
            // if one queue one consumer we don't have to lock queue
            args->one_queue_one_consumer = this->one_queue_one_consumer;
            if (args->end_qid >= num_queues) {
                args->end_qid = num_queues - 1;
            }
            // Here to create threads.
            pthread_create(pthread_id + i, NULL, thread_run, args);
        }
    // All queues are shared by all threads.
    } else {
        this->one_queue_one_consumer = false;
        for(int i = 0; i < num_consumers; i++) 
        {
            struct thread_args *args = new thread_args();
            args->start_qid = 0, args->end_qid = num_queues - 1;
            args->tp = this;
            int res = pthread_create(pthread_id + i, NULL, thread_run, args); 
            // res == 0 means create thread succeed. 
            assert(res == 0);
        }
    }

    // Queue number lager than producer number
    if (num_queues >= num_producers) {
        one_queue_one_producer = true;
    } else {
        one_queue_one_producer = false;
    }
    return true;
}

bool ThreadPool::init() 
{
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
bool ThreadPool::add_worker(uint8_t opt, uint32_t qid, void *object, KEY &key, DATA &data, Status &status) 
{
    // b+tree type threadpool
    if (this->type == TYPE_THREAD_POOL_BTREE) 
    {
        struct request_queue *q_tmp = this->queues[qid];
        bool collision = false;
        uint32_t collision_times = 0;
        assert(q_tmp != NULL);    
        // One thread operates on one or more queues.
        if(one_queue_one_producer)
        {
            while(true)
            {
                uint32_t write_index = q_tmp->write_index;
                uint32_t max_write_index = q_tmp->max_write_index;
                // if is full
                if(index_to_pos(q_tmp, write_index + 1) == index_to_pos(q_tmp, max_write_index)) {
                    collision = true;
                #ifdef QUEUE_OPTIMIZATION
                    collision_times++;
                    if(collision_times > COLLISION_FAIL_TIMES) {
                        return false;
                    }
                #endif
                    continue;
                }
            #ifdef COLLECT_QUEUE
                status.set_queue_collision(collision);
            #endif
                q_tmp->write_index++;
                // add worker
                struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, write_index);
                worker->bp = (BpTree *)object;
                worker->key = key, worker->data = data;
                worker->type = opt;
                __sync_add_and_fetch(&q_tmp->max_read_index, 1);
                break;
            }
        }
        // Multiple threads operate on a single queue.
        // So we need strict lock control.
        else {
            while(true)
            {
                uint32_t write_index, max_write_index;
                bool is_full = false;
                do 
                {
                    write_index = q_tmp->write_index;
                    max_write_index = q_tmp->max_write_index;
                    // if is full
                    if(index_to_pos(q_tmp, write_index + 1) == index_to_pos(q_tmp, max_write_index)) {
                        is_full = true;
                        break;
                    }
                } while(!__sync_bool_compare_and_swap(&q_tmp->write_index, \
                        write_index, write_index + 1)); // update write index
                // queue is full
                if(is_full) {
                    collision = true;
                #ifdef QUEUE_OPTIMIZATION
                    collision_times++;
                    if(collision_times > COLLISION_FAIL_TIMES) {
                        return false;
                    }
                #endif
                    continue;
                }
            #ifdef COLLECT_QUEUE
                status.set_queue_collision(true);
            #endif
                // add worker
                struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, write_index);
                worker->bp = (BpTree *)object;
                worker->key = key, worker->data = data;
                worker->type = opt;
                // update max_read_index
                while(!__sync_bool_compare_and_swap(&q_tmp->max_read_index, \
                        write_index, write_index + 1)) {
                    sched_yield();
                }
                break;
            }
        }
    } else { return false;}
    return true;
}

