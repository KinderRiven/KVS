#include "hthread.h"
using namespace hikv;
using namespace std;

// Structure Function.
// num_queues : num_queues / num_queue_groups
// queue_suze : queue_size / per_queue_size
ThreadPool::ThreadPool(uint8_t type, uint32_t num_producers, uint32_t num_consumers, uint32_t num_queues, uint32_t queue_size) 
{
    this->type = type;
    this->num_producers = num_producers;
    this->num_consumers = num_consumers;
    this->num_queues = num_queues;
    this->queue_size = queue_size;
    this->per_queue_size = queue_size;
    this->num_queue_groups = num_queues;
}

// print queue message
void ThreadPool::Print()
{
    printf("[ThreadPool]:\n");
    printf("[1] num producers : %2d, [2] num_consumers : %2d\n", \
            this->num_producers, this->num_consumers);
#ifdef DYNAMIC_QUEUE
    printf("[3] num groups: %2d, [4] queue size    : %2d\n", \
            this->num_queue_groups, this->per_queue_size);
    printf("[5] initial num queue per group : %2d, totla size : %llu MB\n", \
            INITIAL_NUM_QUEUE_PER_GROUP, \
            (uint64_t)this->num_queue_groups * this->per_queue_size * INITIAL_NUM_QUEUE_PER_GROUP * \
            sizeof(bplus_tree_worker) / (1024 * 1024));
#else
    printf("[3] num queues    : %2d, [4] queue size    : %2d\n", \
            this->num_queues, this->queue_size);
    printf("[5] totla size : %llu MB\n",  \
            (uint64_t)this->num_queues * this->queue_size * sizeof(bplus_tree_worker) / (1024 * 1024));
#endif
    printf("[6] one queue one consumers : ");
    this->one_queue_one_consumer == true ? printf("Yes\n") : printf("No\n");
    printf("[7] one queue one producer  : ");
    this->one_queue_one_producer == true ? printf("Yes\n") : printf("No\n");
    printf("[8] queue optimization : ");
#ifdef QUEUE_OPTIMIZATION
    printf("Yes\n");
#else
    printf("No\n");
#endif
}

ThreadPool::~ThreadPool() 
{
    for(int i = 0; i < num_queues; i++) {
        free(queues[i]);
    }
}

// Function to get queue[i]
struct request_queue *ThreadPool::get_queue(int qid) 
{
    return this->queues[qid];
}

struct queue_group *ThreadPool::get_queue_group(int gid)
{
    return this->queue_groups[gid];
}

// Function to get worker using postion (uint32_t)
static bplus_tree_worker *get_bplus_worker(struct request_queue *queue,  int pos) 
{
    uint32_t p = (pos % queue->size) * sizeof(struct bplus_tree_worker);
    return (bplus_tree_worker*)(queue->data + p);
}

static bplus_tree_worker *get_bplus_worker(struct queue_group *group, int pos)
{
    uint32_t q = (pos / group->per_queue_size) % group->num_queues;
    uint32_t p = (pos % group->per_queue_size) * sizeof(struct bplus_tree_worker);
    return (struct bplus_tree_worker *)((uint8_t*)group->queues[q] + p);
}

// compare group queue position 
// true : equal, false : not equal
static bool compare_group_queue_position(struct queue_group *group, uint32_t pos1, uint32_t pos2)
{
    // the queue member id
    uint32_t q1 = (pos1 / group->per_queue_size) % group->num_queues;
    uint32_t q2 = (pos2 / group->per_queue_size) % group->num_queues;
    // the position in queue
    uint32_t p1 = pos1 % group->per_queue_size;
    uint32_t p2 = pos2 % group->per_queue_size;

    if (q1 == q2 && p1 == p2) {
        return true;
    } else { 
        return false;
    }
}

// compare queue postion
// true : equal, false : not equal
static bool compare_queue_position(struct request_queue *queue, uint32_t pos1, uint32_t pos2)
{
    return (pos1 % queue->size) == (pos2 % queue->size);
}

// change index to postion in queue
static uint32_t index_to_pos(struct request_queue *queue, uint32_t index) 
{
    return index % queue->size;
}

// queue empty
static bool queue_is_empty(struct request_queue *queue) 
{
    if (queue->read_index == queue->max_read_index) {
        return true;
    } else {
        return false;
    }
}

static bool group_is_empty(struct queue_group *group)
{
    if (group->read_index == group->max_read_index) {
        return true;
    } else { 
        return false; 
    }
}

static void process_bplus_request(struct bplus_tree_worker *worker) 
{
    // Do work
    HBpTree *bp = worker->bp;
    bool res = bp->Put(worker->key, worker->data);
    assert(res == true);
}

// Function to process request.
static void process_queue_request(struct thread_args* args) 
{
    // Start to process request.
    while(true) 
    {
        for(int i = args->start_qid; i <= args->end_qid; i++) 
        {
            struct request_queue *q_tmp = args->tp->get_queue(i);
            if (args->one_queue_one_consumer) 
            {
                // current we can read
                uint32_t read_index = q_tmp->read_index;
                if (queue_is_empty(q_tmp))
                    continue;
                q_tmp->read_index++;
                // here to solve bplus work.
                if (args->tp->get_type() == TYPE_THREAD_POOL_BTREE) 
                {
                    struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, read_index);
                    process_bplus_request(worker);
                }
                __sync_add_and_fetch(&(q_tmp->max_write_index), 1);
            }
            else
            {
                bool is_empty = false;
                uint32_t read_index, max_read_index;
                do {
                    read_index = q_tmp->read_index;
                    max_read_index = q_tmp->max_read_index;
                    if(read_index == max_read_index) 
                    {
                        is_empty = true;
                        break;
                    }
                } while(!__sync_bool_compare_and_swap(&q_tmp->read_index, read_index, read_index + 1));
                // if is empty.
                if (is_empty) 
                {
                    continue;
                }
                if (args->tp->get_type() == TYPE_THREAD_POOL_BTREE) 
                {
                    struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, read_index);
                    process_bplus_request(worker);
                }
                while(!__sync_bool_compare_and_swap(&q_tmp->max_write_index, read_index, read_index + 1)) 
                {
                    sched_yield();
                }
            }
        }
    }
}

static void process_group_request(struct thread_args* args)
{
    while(true) 
    {
        for(int i = args->start_qid; i <= args->end_qid; i++) 
        {
            struct queue_group *group_tmp = args->tp->get_queue_group(i);
            if (args->one_queue_one_consumer) 
            {
                // current we can read
                uint32_t read_index = group_tmp->read_index;
                if (group_is_empty(group_tmp)) {
                    continue;
                }
                group_tmp->read_index++;
                // here to solve bplus work.
                if (args->tp->get_type() == TYPE_THREAD_POOL_BTREE) 
                {
                    struct bplus_tree_worker *worker = get_bplus_worker(group_tmp, read_index);
                    process_bplus_request(worker);
                }
                __sync_add_and_fetch(&(group_tmp->max_write_index), 1);
            }
            else 
            {
                bool is_empty = false;
                uint32_t read_index, max_read_index;
                do {
                    read_index = group_tmp->read_index;
                    max_read_index = group_tmp->max_read_index;
                    if (group_is_empty(group_tmp)) 
                    {
                        is_empty = true;
                        break;
                    }
                } while(!__sync_bool_compare_and_swap(&group_tmp->read_index, read_index, read_index + 1));
                // if is empty.
                if (is_empty) 
                {
                    continue;
                }
                if(args->tp->get_type() == TYPE_THREAD_POOL_BTREE) 
                {
                    struct bplus_tree_worker *worker = get_bplus_worker(group_tmp, read_index);
                    process_bplus_request(worker);
                }
                while(!__sync_bool_compare_and_swap(&group_tmp->max_write_index, read_index, read_index + 1)) 
                {
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
#ifdef THREAD_CPU_BIND
    cpu_set_t mask;
    // Here to bind thread into CPU set.
	CPU_ZERO(&mask);
    CPU_SET((int)args->thread_id + args->num_producers, &mask);

    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
		printf("threadpool, set thread affinity failed.\n");
	}
#endif
    // Here to process request.
#ifdef DYNAMIC_QUEUE
    process_group_request(args);
#else
    process_queue_request(args);
#endif
    return NULL;
}

// Function to init queue, include malloc queue space.
bool ThreadPool::init_queue() 
{
    uint32_t malloc_size;
    // head (to record info) + body (to save request)
    if (type == TYPE_THREAD_POOL_BTREE) {
        malloc_size = sizeof(request_queue) + queue_size * sizeof(bplus_tree_worker);
    } else {
        return false;
    }
#ifdef DYNAMIC_QUEUE
    // else if group queue process
    for(int i = 0; i < num_queue_groups; i++)
    {
        queue_groups[i] = (struct queue_group*)malloc(sizeof(queue_group));
        assert(queue_groups[i] != NULL);
        memset(queue_groups[i], 0, sizeof(malloc_size));

        queue_groups[i]->num_queues = INITIAL_NUM_QUEUE_PER_GROUP;
        queue_groups[i]->per_queue_size = this->per_queue_size;

        queue_groups[i]->read_index = queue_groups[i]->max_read_index = 0;
        queue_groups[i]->write_index = queue_groups[i]->max_write_index = 0;

        for(int j = 0; j < queue_groups[i]->num_queues; j++) {
            queue_groups[i]->queues[j] = (void*)malloc(malloc_size);
        }
    }
#else
    // if is queue process
    for(int i = 0; i < num_queues; i++) 
    {
        queues[i] = (struct request_queue*)malloc(malloc_size);
        assert(queues[i] != NULL);
        memset(queues[i], 0 , malloc_size);
        
        queues[i]->size = queue_size;
        queues[i]->read_index = queues[i]->max_read_index = 0;
        queues[i]->write_index = queues[i]->max_write_index = 0;
    }
#endif
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
            // set thread id
            args->thread_id = i;
            args->num_producers = this->num_producers;
            // thread should to scan the queue[start, end]
            args->start_qid = start_qid;
            args->end_qid = start_qid + span - 1;
            // threadpool
            args->tp = this;
            // if one queue one consumer we don't have to lock queue
            args->one_queue_one_consumer = this->one_queue_one_consumer;
            if (args->end_qid >= num_queues) 
            {
                args->end_qid = num_queues - 1;
            }
            // Here to create threads.
            pthread_create(pthread_id + i, NULL, thread_run, args);
        }
    // All queues are shared by all threads.
    } 
    else
    {
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
bool ThreadPool::add_worker_into_queue(uint8_t opt, uint32_t qid, void *object, \
                KEY &key, DATA &data, Status &status) 
{
    // b+tree type threadpool
    if (this->type == TYPE_THREAD_POOL_BTREE) 
    {
        struct request_queue *q_tmp = this->queues[qid];
        bool collision = false;
        volatile uint32_t collision_times = 0;
        assert(q_tmp != NULL); 
        // One thread operates on one or more queues.
        if (one_queue_one_producer)
        {
            while(true)
            {
                volatile uint32_t write_index = q_tmp->write_index;
                volatile uint32_t max_write_index = q_tmp->max_write_index;
                // if is full
                if(compare_queue_position(q_tmp, write_index + 1, max_write_index)) 
                {
                    collision = true;
                #ifdef QUEUE_OPTIMIZATION
                    collision_times++;
                    if (collision_times > COLLISION_FAIL_TIMES) {
                        return false;
                    }
                #endif
                    continue;
                }
            #ifdef COLLECT_QUEUE
                status.queue_collision = collision;
            #endif
                q_tmp->write_index++;
                // add worker
                struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, write_index);
                worker->bp = (HBpTree *)object;
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
                    if(compare_queue_position(q_tmp, write_index + 1, max_write_index)) 
                    {     
                        is_full = true;
                        break;
                    }
                } while(!__sync_bool_compare_and_swap(&q_tmp->write_index, \
                        write_index, write_index + 1)); // update write index
                // queue is full
                if (is_full) 
                {
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
                status.queue_collision = collision;
            #endif
                // add worker
                struct bplus_tree_worker *worker = get_bplus_worker(q_tmp, write_index);
                worker->bp = (HBpTree *)object;
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
    } else { return false;} // other threadpoll type
    return true;
}

// Function to isnert a worker into group.
bool ThreadPool::add_worker_into_group(uint8_t opt, uint32_t qid, void *object, \
                KEY &key, DATA &data, Status &status)
{
    // b+tree type threadpool
    if (this->type == TYPE_THREAD_POOL_BTREE) 
    {
        struct queue_group *group_tmp = this->queue_groups[qid];
        bool collision = false;
        volatile uint32_t collision_times = 0;
        assert(group_tmp != NULL);
        // One thread operates on one or more queues.
        if (one_queue_one_producer)
        {
            while(true)
            {
                volatile uint32_t write_index = group_tmp->write_index;
                volatile uint32_t max_write_index = group_tmp->max_write_index;
                // if is full
                if (compare_group_queue_position(group_tmp, write_index + 1, max_write_index)) 
                {
                    collision = true;
                #ifdef QUEUE_OPTIMIZATION
                    collision_times++;
                    if(collision_times > COLLISION_FAIL_TIMES)
                        return false;
                #endif
                    continue;
                }
            #ifdef COLLECT_QUEUE
                status.queue_collision = collision;
            #endif
                group_tmp->write_index++;
                // add worker
                struct bplus_tree_worker *worker = get_bplus_worker(group_tmp, write_index);
                worker->bp = (HBpTree *)object;
                worker->key = key, worker->data = data;
                worker->type = opt;
                __sync_add_and_fetch(&group_tmp->max_read_index, 1);
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
                    write_index = group_tmp->write_index;
                    max_write_index = group_tmp->max_write_index;
                    // if is full
                    if(compare_group_queue_position(group_tmp, write_index + 1, max_write_index)) {
                        is_full = true;
                        break;
                    }
                } while(!__sync_bool_compare_and_swap(&group_tmp->write_index, \
                        write_index, write_index + 1)); // update write index
                // queue is full
                if (is_full) 
                {
                    collision = true;
                #ifdef QUEUE_OPTIMIZATION
                    collision_times++;
                    if(collision_times > COLLISION_FAIL_TIMES)
                        return false;
                #endif
                    continue;
                }
            #ifdef COLLECT_QUEUE
                status.queue_collision = collision;
            #endif
                // add worker
                struct bplus_tree_worker *worker = get_bplus_worker(group_tmp, write_index);
                worker->bp = (HBpTree *)object;
                worker->key = key, worker->data = data;
                worker->type = opt;
                // update max_read_index
                while(!__sync_bool_compare_and_swap(&group_tmp->max_read_index, write_index, write_index + 1)) 
                {
                    sched_yield();
                }
                break;
            }
        }
    } else { return false; } // other threadpoll type
    return true;
}


